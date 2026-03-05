use std::{cmp, sync::Arc, time::Duration};

use crate::ChainState;

use anyhow::{anyhow, bail, Result};
use contract_interface::da_entrance::{DataUploadFilter, ErasureCommitmentVerifiedFilter};
use ethers::{abi::RawLog, prelude::EthLogDecode, providers::Middleware, types::BlockNumber};
use storage::{
    blob_status_db::{BlobStatus, BlobStatusDB},
    misc_db::MiscDB,
};
use tokio::time::sleep;
use utils::metrics;

const MAX_LOGS_PAGINATION: u64 = 100;

pub async fn start_da_monitor(chain_state: Arc<ChainState>, start_block_number: u64) -> Result<()> {
    let maybe_progress = chain_state.db.read().await.get_sync_progress().await?;
    match maybe_progress {
        Some(_) => {}
        None => {
            chain_state
                .db
                .write()
                .await
                .put_sync_progress(start_block_number)
                .await?;
        }
    }
    tokio::spawn(async move {
        loop {
            match check_da_logs(chain_state.clone()).await {
                Ok(_) => {}
                Err(e) => {
                    error!("poll da logs error: {:?}", e);
                }
            }
            sleep(Duration::from_secs(5)).await;
        }
    });
    Ok(())
}

async fn check_da_logs(chain_state: Arc<ChainState>) -> Result<()> {
    let from = chain_state
        .db
        .read()
        .await
        .get_sync_progress()
        .await?
        .unwrap();
    match chain_state
        .provider
        .get_block(BlockNumber::Finalized)
        .await?
    {
        Some(b) => {
            if let Some(bn) = b.number {
                let to = bn.as_u64();
                if to >= from {
                    info!(
                        "checking da entrance logs from {:?} to {:?} block..",
                        from, to
                    );
                    check_data_logs(chain_state.clone(), from, to).await?;
                    metrics::CHAIN_PROGRESS.set(to as f64);
                    chain_state
                        .db
                        .write()
                        .await
                        .put_sync_progress(to + 1)
                        .await?;
                }
            } else {
                bail!(anyhow!("block number is empty"));
            }
        }
        None => {
            bail!(anyhow!("finalized block returns None"));
        }
    }
    Ok(())
}

async fn check_data_logs(chain_state: Arc<ChainState>, from: u64, to: u64) -> Result<()> {
    let mut l = from;
    while l <= to {
        let r = cmp::min(l + MAX_LOGS_PAGINATION, to);
        check_data_upload(chain_state.clone(), l, r).await?;
        check_data_verified(chain_state.clone(), l, r).await?;
        l = r + 1;
    }
    Ok(())
}

async fn check_data_upload(chain_state: Arc<ChainState>, l: u64, r: u64) -> Result<()> {
    // Fetch epoch info to validate window
    let current_epoch = chain_state.da_signers.epoch_number().call().await?.as_u64();
    let epoch_window_size = chain_state.da_entrance.epoch_window_size().call().await?.as_u64();

    let filter: ethers::types::Filter = chain_state
        .da_entrance
        .data_upload_filter()
        .from_block(l)
        .to_block(r)
        .address(chain_state.da_entrance.address().into())
        .filter;

    for log in chain_state.provider.get_logs(&filter).await? {
        match DataUploadFilter::decode_log(&RawLog {
            topics: log.topics,
            data: log.data.to_vec(),
        }) {
            Ok(event) => {
                let epoch = event.epoch.as_u64();
                
                // Skip if epoch is too old (already pruned or will be soon)
                if epoch + epoch_window_size < current_epoch {
                    continue;
                }

                let quorum_id = event.quorum_id.as_u64();
                let maybe_blob_status = chain_state
                    .db
                    .read()
                    .await
                    .get_blob_status(epoch, quorum_id, event.data_root)
                    .await?;
                match maybe_blob_status {
                    Some(_) => {}
                    None => {
                        chain_state
                            .db
                            .write()
                            .await
                            .put_blob(epoch, quorum_id, event.data_root, BlobStatus::UPLOADED)
                            .await?;
                        info!(
                            "new file found, epoch: {:?}, quorum_id: {:?}, data_root: {:X?}",
                            epoch, quorum_id, event.data_root
                        );
                    }
                }
            }
            Err(e) => {
                error!("log decode error: e={:?}", e);
            }
        }
    }
    Ok(())
}

async fn check_data_verified(chain_state: Arc<ChainState>, l: u64, r: u64) -> Result<()> {
    // Fetch epoch info to validate window
    let current_epoch = chain_state.da_signers.epoch_number().call().await?.as_u64();
    let epoch_window_size = chain_state.da_entrance.epoch_window_size().call().await?.as_u64();

    let filter: ethers::types::Filter = chain_state
        .da_entrance
        .erasure_commitment_verified_filter()
        .from_block(l)
        .to_block(r)
        .address(chain_state.da_entrance.address().into())
        .filter;

    for log in chain_state.provider.get_logs(&filter).await? {
        match ErasureCommitmentVerifiedFilter::decode_log(&RawLog {
            topics: log.topics,
            data: log.data.to_vec(),
        }) {
            Ok(event) => {
                let epoch = event.epoch.as_u64();

                // Skip if epoch is too old
                if epoch + epoch_window_size < current_epoch {
                    continue;
                }

                let quorum_id = event.quorum_id.as_u64();
                let maybe_blob_status = chain_state
                    .db
                    .read()
                    .await
                    .get_blob_status(epoch, quorum_id, event.data_root)
                    .await?;
                let mut need_write = true;
                if let Some(BlobStatus::VERIFIED) = maybe_blob_status {
                    need_write = false;
                }
                if need_write {
                    chain_state
                        .db
                        .write()
                        .await
                        .put_blob(epoch, quorum_id, event.data_root, BlobStatus::VERIFIED)
                        .await?;
                    info!(
                        "file verified, epoch: {:?}, quorum_id: {:?}, data_root: {:X?}",
                        epoch, quorum_id, event.data_root
                    );
                }
            }
            Err(e) => {
                error!("log decode error: e={:?}", e);
            }
        }
    }
    Ok(())
}
