use anyhow::{anyhow, bail, Result};

use chain_utils::DefaultMiddleware;
use ethers::types::{H160, U256};
use ethers::{
    providers::Middleware,
    signers::{LocalWallet, Signer},
    types::TransactionRequest,
};

#[derive(Debug, Clone)]
pub enum TransactionInfo {
    RegisterSigner(H160),
    RegisterEpoch(H160, u64),
    UpdateSocket(H160, String),
    Stake(H160, U256),
    SyncFixedTimes(U256),
}

pub struct Transactor {
    signer: LocalWallet,
    client: DefaultMiddleware,
}

impl Transactor {
    pub fn new(middleware: DefaultMiddleware) -> Result<Self> {
        Ok(Self {
            signer: middleware.signer().clone(),
            client: middleware,
        })
    }

    pub fn signer_address(&self) -> H160 {
        self.signer.address()
    }

    // return continue(true) or break(false)
    fn handle_send_error(&self, e_str: &str, tx_info: TransactionInfo) -> bool {
        if e_str.contains("max fee per gas less than block base fee") {
            info!("gas price too low, resending..");
            return true;
        }
        if e_str.contains("insufficient funds for transfer") {
            warn!(
                "sender {:?} balance is insufficient.",
                self.signer.address()
            );
            return false;
        }
        info!(
            "transaction {:?} will revert: {:?}, skipped.",
            tx_info, e_str,
        );
        false
    }

    pub async fn send(
        &self,
        tx_no_sender: TransactionRequest,
        tx_info: TransactionInfo,
    ) -> Result<bool> {
        let tx = tx_no_sender.clone().from(self.signer.address());
        loop {
            match self.client.send_transaction(tx.clone(), None).await {
                Ok(pending_tx) => {
                    let hash = pending_tx.tx_hash();
                    info!(
                        "new transaction sent with hash {:?}, tx_info: {:?}",
                        hash, tx_info,
                    );
                    let mut status = 2;
                    match pending_tx.await {
                        Ok(maybe_receipt) => {
                            if let Some(receipt) = maybe_receipt {
                                if let Some(x) = receipt.status {
                                    status = x.as_u32();
                                }
                            }
                        }
                        Err(e) => {
                            info!("transaction {:?} error: {:?}", hash, e);
                        }
                    }
                    match status {
                        0 => {
                            info!("transaction {:?} failed.", hash);
                            return Ok(false);
                        }
                        1 => {
                            info!("transaction {:?} success.", hash)
                        }
                        2 => {
                            info!("transaction {:?} confirmed, status unknown.", hash)
                        }
                        _ => {}
                    }
                    return Ok(true);
                }
                Err(e) => {
                    let e_str = e.to_string();
                    if self.handle_send_error(&e_str, tx_info.clone()) {
                        continue;
                    } else {
                        bail!(anyhow!(e_str));
                    }
                }
            }
        }
    }
}
