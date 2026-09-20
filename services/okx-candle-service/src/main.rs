use anyhow::Result;
use clap::Parser;
use okx_candle_service::{cache::Cache, collector, config::Args, server, state::State};
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
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    eprintln!(
        "okx-candle-service listening={} pairs={} history={}",
        listener.local_addr()?,
        pairs.len(),
        args.history
    );
    let stop = CancellationToken::new();
    let cache_stop = CancellationToken::new();
    let cache_task = cache.map(|cache| tokio::spawn(cache.run(state.clone(), cache_stop.clone())));
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
    let result = axum::serve(listener, server::router(state, token, stop.clone()))
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
    collector_result??;
    result?;
    Ok(())
}
