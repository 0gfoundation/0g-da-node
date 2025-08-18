#[macro_use]
extern crate tracing;

mod config;
mod context;
mod runtime;

use std::{error::Error, net::SocketAddr, str::FromStr, sync::Arc};

use anyhow::{anyhow, Result};

use chain_state::transactor::Transactor;
use chain_state::{
    da_handler::start_da_monitor, signers_handler::start_epoch_registration, ChainState,
};
use chain_utils::make_provider;
use da_miner::DasMineService;
use grpc::run_server;
use pruner::run_pruner;

use prometheus_exporter::Exporter;
use runtime::Environment;
use task_executor::TaskExecutor;
use tokio::sync::Mutex;
use tracing::Level;
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::context::Context;
use crate::runtime::make_environment;

async fn start_grpc_server(chain_state: Arc<ChainState>, ctx: &Context) -> Result<()> {
    let db = ctx.db.clone();
    let signer_bls_private_key = ctx.config.signer_bls_private_key;
    let grpc_listen_address = ctx.config.grpc_listen_address.clone();
    let encoder_params_dir = ctx.config.encoder_params_dir.clone();
    let max_ongoing_sign_request = ctx.config.max_ongoing_sign_request;
    info!("starting grpc server at {:?}", grpc_listen_address);
    tokio::spawn(async move {
        run_server(
            db,
            chain_state,
            signer_bls_private_key,
            SocketAddr::from_str(&grpc_listen_address).unwrap(),
            encoder_params_dir,
            max_ongoing_sign_request,
        )
        .await
        .map_err(|e| anyhow!(e.to_string()))
        .unwrap();
    });
    Ok(())
}

async fn start_pruner(chain_state: Arc<ChainState>, ctx: &Context) -> Result<()> {
    let db = ctx.db.clone();
    tokio::spawn(async move {
        run_pruner(db, chain_state)
            .await
            .map_err(|e| anyhow!(e.to_string()))
            .unwrap();
    });
    Ok(())
}

async fn setup_chain_state(ctx: &Context) -> Result<Arc<ChainState>> {
    let chain_state = Arc::new(
        ChainState::new(
            &ctx.config.eth_rpc_url,
            ctx.config.da_entrance_address,
            ctx.transactor.clone(),
            ctx.db.clone(),
        )
        .await?,
    );
    chain_state.check_staked(ctx.config.amount_to_stake).await?;
    chain_state
        .check_signer_registration(
            ctx.config.signer_bls_private_key,
            ctx.config.socket_address.clone(),
        )
        .await?;
    start_epoch_registration(chain_state.clone(), ctx.config.signer_bls_private_key);
    start_da_monitor(chain_state.clone(), ctx.config.start_block_number).await?;
    Ok(chain_state)
}

async fn start_server(ctx: &Context) -> Result<()> {
    let chain_state = setup_chain_state(ctx).await?;
    start_grpc_server(chain_state.clone(), ctx).await?;
    start_pruner(chain_state.clone(), ctx).await?;
    Ok(())
}

async fn start_das_service(executor: TaskExecutor, ctx: &Context) {
    if !ctx.config.enable_das {
        return;
    }
    let provider = make_provider(&ctx.config.eth_rpc_url, &ctx.config.miner_eth_private_key)
        .await
        .unwrap();
    let transactor: Arc<Mutex<Transactor>> = Arc::new(Mutex::new(
        Transactor::new(provider.clone(), ctx.config.min_gas_price).unwrap(),
    ));
    DasMineService::spawn(
        executor,
        provider,
        ctx.config.da_entrance_address,
        ctx.config.das_test,
        ctx.db.clone(),
        transactor,
    )
    .await
    .unwrap();
    info!("DA sampling mine service started");
}

async fn start_metrics_exporter(ctx: &Context) -> Result<Exporter> {
    Ok(prometheus_exporter::start(
        ctx.config
            .prometheus_exporter_address
            .parse()
            .expect("failed to parse binding"),
    )?)
}

fn main() -> Result<(), Box<dyn Error>> {
    // enable backtraces
    std::env::set_var("RUST_BACKTRACE", "1");

    let (environment, runtime, executor) = make_environment().unwrap();

    let res = runtime.block_on(async { async_main(environment, executor).await });

    if let Err(e) = res {
        error!(reason =?e, "Service exit");
    }

    runtime.shutdown_timeout(std::time::Duration::from_secs(15));
    info!("Stopped");

    Ok(())
}

async fn async_main(
    environment: Environment,
    executor: TaskExecutor,
) -> Result<(), Box<dyn Error>> {
    // CLI, config
    let config = Config::from_cli_file().unwrap();

    // tracing

    // make sure log level is valid string
    let _ = Level::from_str(&config.log_level)?;
    let filter = EnvFilter::try_new(format!("{},hyper=warn", config.log_level))?;
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let ctx = Context::new(config).await?;

    // rayon
    if let Some(num_threads) = ctx.config.max_verify_threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build_global()?;
    }

    let (_das_res, rpc_res, _exporter) = tokio::join!(
        start_das_service(executor, &ctx),
        start_server(&ctx),
        start_metrics_exporter(&ctx)
    );

    if !ctx.config.das_test {
        rpc_res?;
    }

    environment.wait_shutdown_signal().await;

    info!("Signal received, stopping..");
    Ok(())
}
