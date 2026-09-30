use anyhow::Result;
use clap::Parser;
use okx_candle_service::{
    cache::Cache,
    collector,
    config::Args,
    market_picture::{Projection, ProjectionCache, SharedProjection},
    server,
    state::State,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let (pairs, token) = args.validate()?;
    let state = State::new(&pairs, args.history);
    let cache = args.cache_dir.as_deref().map(Cache::open).transpose()?;
    if let Some(cache) = &cache {
        cache.load_into(&state).await?;
    }
    let (market_picture, market_picture_cache) = if args.market_picture {
        let cache_root = args
            .cache_dir
            .as_deref()
            .expect("validated market picture cache root");
        let projection_cache = ProjectionCache::open(cache_root)?;
        let mut projection = Projection::new(
            &pairs,
            (okx_candle_service::model::now() * 1000.0).round() as u64,
        );
        projection_cache.load_all(&mut projection, &pairs)?;
        (
            Some(Arc::new(Mutex::new(projection)) as SharedProjection),
            Some(projection_cache),
        )
    } else {
        (None, None)
    };
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    eprintln!(
        "okx-candle-service listening={} pairs={} history={}",
        listener.local_addr()?,
        pairs.len(),
        args.history
    );
    let stop = CancellationToken::new();
    let cache_stop = CancellationToken::new();
    let cache_task = cache
        .clone()
        .map(|cache| tokio::spawn(cache.run(state.clone(), cache_stop.clone())));
    let market_picture_task = match (&market_picture, market_picture_cache) {
        (Some(projection), Some(projection_cache)) => {
            Some(tokio::spawn(okx_candle_service::market_picture::run(
                projection.clone(),
                state.clone(),
                projection_cache,
                cache_stop.clone(),
            )))
        }
        _ => None,
    };
    let signal_stop = stop.clone();
    tokio::spawn(async move {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
        signal_stop.cancel();
    });
    let collector_stop = stop.clone();
    let collector_state = state.clone();
    let collector = tokio::spawn(async move {
        let result = collector::run(collector_state, pairs, collector_stop.clone()).await;
        collector_stop.cancel();
        result
    });
    let result = axum::serve(
        listener,
        server::router(state, token, stop.clone(), market_picture),
    )
    .with_graceful_shutdown(stop.clone().cancelled_owned())
    .await;
    stop.cancel();
    // Stop all upstream mutation before taking the cache's final snapshot. Preserve
    // cleanup even if the collector returned an error or its task panicked.
    let collector_result = collector.await;
    cache_stop.cancel();
    if let Some(cache_task) = cache_task {
        cache_task.await?;
    }
    if let Some(market_picture_task) = market_picture_task {
        market_picture_task.await?;
    }
    collector_result??;
    result?;
    Ok(())
}
