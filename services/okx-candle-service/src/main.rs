use anyhow::Result;
use clap::Parser;
use okx_candle_service::{
    cache::Cache,
    collector,
    config::Args,
    market_picture::{Projection, ProjectionCache, SharedProjection},
    observation_config::ObservationSettings,
    server,
    state::State,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

fn observation_extension(
    path: Option<&std::path::Path>,
    pairs: &[String],
) -> (
    Option<ObservationSettings>,
    Option<Arc<okx_candle_service::observation_capture::CaptureState>>,
) {
    let Some(path) = path else {
        return (None, None);
    };
    let prepared = ObservationSettings::read(path, pairs).and_then(|settings| {
        let state = okx_candle_service::observation_capture::CaptureState::new(&settings)?;
        Ok((settings, state))
    });
    match prepared {
        Ok((settings, state)) => (Some(settings), Some(state)),
        Err(error) => {
            eprintln!("observation configuration rejected; candle service continues: {error:#}");
            (
                None,
                Some(
                    okx_candle_service::observation_capture::CaptureState::unavailable(format!(
                        "{error:#}"
                    )),
                ),
            )
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let (pairs, auth) = args.validate()?;
    let (observations, observation_state) =
        observation_extension(args.observations_file.as_deref(), &pairs);
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
    let observation_task =
        observations
            .zip(observation_state.clone())
            .map(|(settings, capture_state)| {
                // An observation failure pauses only this optional extension.
                let capture_stop = stop.child_token();
                tokio::spawn(async move {
                    let result = okx_candle_service::observation_capture::run_with_state(
                        settings,
                        capture_state.clone(),
                        capture_stop,
                    )
                    .await;
                    if let Err(error) = &result {
                        eprintln!("observation capture paused: {error:#}");
                        if let Ok(mut health) = capture_state.health_snapshot() {
                            health.running = false;
                            health.last_error =
                                Some(format!("{error:#}").chars().take(240).collect());
                            if let Err(status_error) = capture_state.set_health(health) {
                                eprintln!(
                                    "observation failure status unavailable: {status_error:#}"
                                );
                            }
                        }
                    }
                    result
                })
            });
    let status_task = args.status_file.clone().map(|path| {
        tokio::spawn(okx_candle_service::status::run(
            state.clone(),
            path,
            cache_stop.clone(),
        ))
    });
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
        server::router_with_observations(
            state,
            auth,
            stop.clone(),
            market_picture,
            observation_state,
        ),
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
    if let Some(status_task) = status_task {
        status_task.await?;
    }
    if let Some(market_picture_task) = market_picture_task {
        market_picture_task.await?;
    }
    if let Some(task) = observation_task {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => eprintln!("observation extension stopped with error: {error:#}"),
            Err(error) => eprintln!("observation extension task failed: {error}"),
        }
    }
    collector_result??;
    result?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optional_extension_absent_has_no_state_or_task_configuration() {
        let (settings, state) = observation_extension(None, &["BTC-USDC".into()]);
        assert!(settings.is_none() && state.is_none());
    }

    #[test]
    fn missing_optional_settings_returns_visible_disabled_state_without_failing_candles() {
        let missing =
            std::env::temp_dir().join(format!("absent-observation-{}.json", uuid::Uuid::new_v4()));
        let (settings, state) = observation_extension(Some(&missing), &["BTC-USDC".into()]);
        assert!(settings.is_none());
        let state = state.unwrap();
        let health = state.health_snapshot().unwrap();
        assert!(!health.enabled && !health.running && health.last_error.is_some());
        assert!(state.configured_pairs().is_empty());
    }
}
