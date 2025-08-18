use anyhow::Result;
use chain_state::transactor::Transactor;
use chain_utils::DefaultMiddleware;
use std::sync::Arc;
use storage::Storage;
use tokio::sync::{Mutex, RwLock};

use crate::config::Config;

pub struct Context {
    pub config: Config,
    pub transactor: Arc<Mutex<Transactor>>,
    pub db: Arc<RwLock<Storage>>,
    pub provider: DefaultMiddleware,
}

impl Context {
    pub async fn new(config: Config) -> Result<Self> {
        let provider =
            chain_utils::make_provider(&config.eth_rpc_url, &config.signer_eth_private_key)
                .await
                .unwrap();
        let transactor: Arc<Mutex<Transactor>> = Arc::new(Mutex::new(
            Transactor::new(provider.clone(), config.min_gas_price).unwrap(),
        ));
        // db
        let db = Arc::new(RwLock::new(Storage::new(&config.data_path).unwrap()));

        Ok(Self {
            config,
            transactor,
            db,
            provider,
        })
    }
}
