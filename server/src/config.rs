use std::str::FromStr;

use anyhow::{anyhow, bail, Result};
use ark_bn254::Fr;

use config::ConfigError::NotFound;
use ethers::{
    abi::Address,
    types::{H160, H256, U256},
};

mod cli {
    use clap::{arg, command, Command};

    pub fn cli_app<'a>() -> Command<'a> {
        command!()
            .arg(arg!(-c --config <FILE> "Sets a custom config file"))
            .allow_external_subcommands(true)
    }
}

struct RawConfig(config::Config);

impl RawConfig {
    fn get_string(&self, key: &'static str) -> Result<String> {
        self.0
            .get_string(key)
            .map_err(|e| anyhow!("Cannot parse config key `{}` as string: {:?}", key, e))
    }

    fn get_u64(&self, key: &'static str) -> Result<u64> {
        self.0
            .get_int(key)
            .map(|x| x as u64)
            .map_err(|e| anyhow!("Cannot parse config key `{}` as int: {:?}", key, e))
    }

    fn get_address(&self, key: &'static str) -> Result<Address> {
        Address::from_str(&self.get_string(key)?)
            .map_err(|err| anyhow!("Cannot parse config key `{}` as address: {:?}", key, err))
    }

    fn get_bytes32(&self, key: &'static str) -> Result<H256> {
        H256::from_str(&self.get_string(key)?)
            .map_err(|err| anyhow!("Cannot parse config key `{}` as bytes32: {:?}", key, err))
    }

    fn get_bls_key(&self, key: &'static str) -> Result<Fr> {
        Fr::from_str(&self.get_string(key)?)
            .map_err(|err| anyhow!("Cannot parse config key `{}` as bls key: {:?}", key, err))
    }

    fn get_u64_opt(&self, key: &'static str) -> Result<Option<u64>> {
        match self.0.get_int(key) {
            Ok(x) => Ok(Some(x as u64)),
            Err(NotFound(_)) => Ok(None),
            Err(e) => Err(anyhow!("Cannot parse config key `{}` as int: {:?}", key, e)),
        }
    }

    fn get_bool_opt(&self, key: &'static str) -> Result<bool> {
        match self.0.get_bool(key) {
            Ok(x) => Ok(x),
            Err(NotFound(_)) => Ok(false),
            Err(e) => Err(anyhow!(
                "Cannot parse config key `{}` as bool: {:?}",
                key,
                e
            )),
        }
    }
}

pub struct Config {
    pub log_level: String,
    pub encoder_params_dir: String,
    pub grpc_listen_address: String,
    pub max_ongoing_sign_request: Option<u64>,
    pub max_verify_threads: Option<usize>,
    pub socket_address: String,
    pub eth_rpc_url: String,
    pub start_block_number: u64,
    pub da_entrance_address: H160,
    pub signer_bls_private_key: Fr,
    pub signer_eth_private_key: H256,
    pub miner_eth_private_key: H256,
    pub data_path: String,
    pub enable_das: bool,
    pub das_test: bool,
    pub prometheus_exporter_address: String,
    pub amount_to_stake: U256,
    pub min_gas_price: U256,
}

impl Config {
    pub fn from_cli_file() -> Result<Self> {
        let matches = cli::cli_app().get_matches();
        let c = if let Some(config_file) = matches.value_of("config") {
            RawConfig(
                config::Config::builder()
                    .add_source(config::File::with_name(config_file))
                    .build()?,
            )
        } else {
            bail!(anyhow!("Config file missing!"));
        };

        let enable_das = c.get_bool_opt("enable_das")?;

        Ok(Self {
            enable_das: c.get_bool_opt("enable_das")?,
            das_test: c.get_bool_opt("das_test")?,
            log_level: c.get_string("log_level")?,
            encoder_params_dir: c.get_string("encoder_params_dir")?,
            grpc_listen_address: c.get_string("grpc_listen_address")?,
            max_ongoing_sign_request: c.get_u64_opt("max_ongoing_sign_request")?,
            max_verify_threads: c.get_u64_opt("max_verify_threads")?.map(|x| x as usize),
            socket_address: c.get_string("socket_address")?,
            eth_rpc_url: c.get_string("eth_rpc_endpoint")?,
            start_block_number: c.get_u64("start_block_number")?,
            da_entrance_address: c.get_address("da_entrance_address")?,
            signer_bls_private_key: c.get_bls_key("signer_bls_private_key")?,
            signer_eth_private_key: c.get_bytes32("signer_eth_private_key")?,
            miner_eth_private_key: if enable_das {
                c.get_bytes32("miner_eth_private_key")
                    .or_else(|_| c.get_bytes32("signer_eth_private_key"))?
            } else {
                H256::zero()
            },
            data_path: c.get_string("data_path")?,
            prometheus_exporter_address: c.get_string("prometheus_exporter_address")?,
            amount_to_stake: U256::from(c.get_u64("amount_to_stake")?) * U256::exp10(18),
            min_gas_price: U256::from_dec_str(&c.get_string("min_gas_price")?)?,
        })
    }
}
