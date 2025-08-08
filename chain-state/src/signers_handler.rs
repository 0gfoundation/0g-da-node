use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, bail, Result};
use ark_bn254::{g1, g2, Fr, G1Affine, G2Affine};
use ark_ec::{AffineRepr, CurveGroup};

use ark_serialize::CanonicalSerialize;
use contract_interface::da_registry::{G1Point, G2Point, SignerDetail};

use ethers::{
    providers::Middleware,
    types::{BlockNumber, TransactionRequest, H160, U256},
    utils::keccak256,
};

use storage::quorum_db::{AssignedSlices, QuorumDB};

use tokio::time::sleep;
use utils::{left_pad_zeros, map_to_g1, metrics};

use crate::{transactor::TransactionInfo, ChainState};

const PUBKEY_REGISTRATION_DOMAIN: &[u8] = "0G_BN254_Pubkey_Registration".as_bytes();
const MAX_SYNC_TIMES: u64 = 10;

pub fn serialize_g1_point(point: G1Affine) -> G1Point {
    let mut value: Vec<u8> = Vec::new();
    point
        .x()
        .unwrap()
        .serialize_uncompressed(&mut value)
        .unwrap();
    let x = U256::from_little_endian(&value);
    value = Vec::new();
    point
        .y()
        .unwrap()
        .serialize_uncompressed(&mut value)
        .unwrap();
    let y = U256::from_little_endian(&value);
    G1Point { x, y }
}

pub fn serialize_g2_point(point: G2Affine) -> G2Point {
    let mut value: Vec<u8> = Vec::new();
    point
        .x()
        .unwrap()
        .serialize_uncompressed(&mut value)
        .unwrap();
    let x = [
        U256::from_little_endian(&value[0..32]),
        U256::from_little_endian(&value[32..64]),
    ];

    value = Vec::new();
    point
        .y()
        .unwrap()
        .serialize_uncompressed(&mut value)
        .unwrap();
    let y = [
        U256::from_little_endian(&value[0..32]),
        U256::from_little_endian(&value[32..64]),
    ];
    G2Point { x, y }
}

fn signer_registration_hash(signer_address: H160, chain_id: u64) -> G1Affine {
    let mut message = vec![];
    message.append(&mut signer_address.to_fixed_bytes().to_vec());
    message.append(&mut left_pad_zeros(chain_id, 32));
    message.append(&mut PUBKEY_REGISTRATION_DOMAIN.to_vec());
    map_to_g1(keccak256(message).to_vec())
}

fn epoch_registration_hash(signer_address: H160, epoch: u64, chain_id: u64) -> G1Affine {
    let mut message = vec![];
    message.append(&mut signer_address.to_fixed_bytes().to_vec());
    message.append(&mut left_pad_zeros(epoch, 8));
    message.append(&mut left_pad_zeros(chain_id, 32));
    map_to_g1(keccak256(message).to_vec())
}

impl ChainState {
    pub async fn check_staked(&self, amount_to_stake: U256) -> Result<()> {
        let staked = self.da_registry.staked(self.signer_address).call().await?;
        if staked < amount_to_stake {
            let to_stake = amount_to_stake - staked;
            let maybe_input_data = self.da_registry.stake().calldata();
            if let Some(input_data) = maybe_input_data {
                info!(
                    "staking amount {:?} for account {:?}",
                    self.signer_address, to_stake
                );
                let tx_request = TransactionRequest::new()
                    .to(self.da_registry.address())
                    .value(to_stake)
                    .data(input_data);
                match self
                    .transactor
                    .lock()
                    .await
                    .send(
                        tx_request,
                        TransactionInfo::Stake(self.signer_address, to_stake),
                    )
                    .await
                {
                    Ok(success) => {
                        if success {
                            info!(
                                "staked {:?} for account {:?}",
                                to_stake, self.signer_address
                            );
                            sleep(Duration::from_secs(10)).await;
                        } else {
                            bail!(anyhow!("stake failed"));
                        }
                    }
                    Err(e) => {
                        bail!(anyhow!(e));
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn check_signer_registration(
        &self,
        signer_bls_private_key: Fr,
        socket: String,
    ) -> Result<()> {
        if !self
            .da_signers
            .is_signer(self.signer_address)
            .call()
            .await?
        {
            let signer_pub_key_g1 =
                (g1::G1Affine::generator() * signer_bls_private_key).into_affine();
            let signer_pub_key_g2 =
                (g2::G2Affine::generator() * signer_bls_private_key).into_affine();
            let hash = signer_registration_hash(
                self.signer_address,
                self.provider.get_chainid().await?.as_u64(),
            );
            let signature = (hash * signer_bls_private_key).into_affine();
            let maybe_input_data = self
                .da_registry
                .register_signer(
                    SignerDetail {
                        signer: self.signer_address,
                        socket: socket.clone(),
                        pk_g1: serialize_g1_point(signer_pub_key_g1),
                        pk_g2: serialize_g2_point(signer_pub_key_g2),
                    },
                    serialize_g1_point(signature),
                )
                .calldata();
            if let Some(input_data) = maybe_input_data {
                info!(
                    "try to register signer: account {:?}, pubkey g1 {:?}, pubkey g2: {:?}, socket: {:?}",
                    self.signer_address,
                    signer_pub_key_g1,
                    signer_pub_key_g2,
                    socket,
                );
                let tx_request = TransactionRequest::new()
                    .to(self.da_registry.address())
                    .data(input_data);
                match self
                    .transactor
                    .lock()
                    .await
                    .send(
                        tx_request,
                        TransactionInfo::RegisterSigner(self.signer_address),
                    )
                    .await
                {
                    Ok(success) => {
                        if success {
                            info!("signer registered");
                            sleep(Duration::from_secs(10)).await;
                        } else {
                            bail!(anyhow!("register signer failed"));
                        }
                    }
                    Err(e) => {
                        bail!(anyhow!(e));
                    }
                }
            }
        }
        match self
            .da_signers
            .get_signer(vec![self.signer_address])
            .call()
            .await?
            .first()
        {
            Some(signer_detail) => {
                if signer_detail.socket != socket {
                    info!(
                        "change socket of signer from {:?} to {:?}",
                        signer_detail.socket, socket
                    );
                    let input_data = self
                        .da_signers
                        .update_socket(socket.clone())
                        .calldata()
                        .unwrap();
                    let tx_request = TransactionRequest::new()
                        .to(self.da_signers.address())
                        .data(input_data);

                    match self
                        .transactor
                        .lock()
                        .await
                        .send(
                            tx_request,
                            TransactionInfo::UpdateSocket(self.signer_address, socket.clone()),
                        )
                        .await
                    {
                        Ok(success) => {
                            if success {
                                info!("socket updated to {:?}", socket.clone());
                                return Ok(());
                            }
                            bail!(anyhow!("update socket failed"));
                        }
                        Err(e) => {
                            bail!(anyhow!(e));
                        }
                    }
                }
            }
            None => {
                bail!("cannot get signer from precompile!")
            }
        }
        Ok(())
    }

    pub async fn fetch_quorum_if_missing(&self, epoch: u64) -> Result<u64> {
        let max_epoch = (self.da_signers.epoch_number().call().await?).as_u64();
        if max_epoch < epoch {
            bail!(anyhow!("invalid epoch"));
        }
        let maybe_quorum_num = self.db.read().await.get_quorum_num(epoch).await?;
        match maybe_quorum_num {
            Some(cnt) => Ok(cnt),
            None => {
                info!("updating quorums of epoch: {:?}", epoch);
                let quorum_cnt = (self
                    .da_signers
                    .quorum_count(U256::from(epoch))
                    .call()
                    .await?)
                    .as_u32() as i32;
                let mut assigned = vec![];
                metrics::EPOCH_QUORUMS.set(quorum_cnt as i64);
                for i in 0..quorum_cnt {
                    let quorum = self
                        .da_signers
                        .get_quorum(U256::from(epoch), U256::from(i))
                        .call()
                        .await?;
                    let assigned_slices: Vec<u64> = quorum
                        .into_iter()
                        .enumerate()
                        .filter(|&(_, signer)| signer == self.signer_address)
                        .map(|(idx, _)| idx as u64)
                        .collect();
                    assigned.push(AssignedSlices(assigned_slices));
                }
                self.db.write().await.put_quorums(epoch, assigned).await?;
                Ok(quorum_cnt as u64)
            }
        }
    }
}

pub fn start_epoch_registration(chain_state: Arc<ChainState>, signer_bls_private_key: Fr) {
    tokio::spawn(async move {
        loop {
            match check_epoch(chain_state.clone(), signer_bls_private_key).await {
                Ok(_) => {}
                Err(e) => {
                    error!("poll check_new_epoch error: {:?}", e);
                }
            }
            sleep(Duration::from_secs(5)).await;
        }
    });
}

async fn check_epoch(chain_state: Arc<ChainState>, signer_bls_private_key: Fr) -> Result<()> {
    match chain_state
        .provider
        .get_block(BlockNumber::Finalized)
        .await?
    {
        Some(b) => {
            if let Some(bn) = b.number {
                let epoch = (chain_state
                    .da_signers
                    .epoch_number()
                    .block(bn)
                    .call()
                    .await?)
                    .as_u64();
                check_new_quorums(chain_state.clone(), epoch).await?;
                check_new_registration(chain_state.clone(), signer_bls_private_key, epoch + 1)
                    .await?;
                check_sample_round(chain_state.clone(), U256::from(bn.as_u64())).await?;
                Ok(())
            } else {
                bail!(anyhow!("block number is empty"));
            }
        }
        None => {
            bail!(anyhow!("finalized block returns None"));
        }
    }
}

async fn check_sample_round(chain_state: Arc<ChainState>, latest_block: U256) -> Result<()> {
    let next_sample_height = chain_state.da_entrance.next_sample_height().call().await?;
    if next_sample_height <= latest_block {
        let sample_period = chain_state.da_entrance.sample_period().call().await?;
        let times = ((latest_block - next_sample_height + 1) / sample_period)
            .min(U256::from(MAX_SYNC_TIMES));
        let maybe_input_data = chain_state.da_entrance.sync_fixed_times(times).calldata();
        if let Some(input_data) = maybe_input_data {
            info!(
                "sync {:?} times to update sample round, next_sample_height: {:?}, latest_block: {:?}",
                times, next_sample_height, latest_block
            );
            let tx_request = TransactionRequest::new()
                .to(chain_state.da_entrance.address())
                .data(input_data);
            match chain_state
                .transactor
                .lock()
                .await
                .send(tx_request, TransactionInfo::SyncFixedTimes(times))
                .await
            {
                Ok(success) => {
                    if success {
                        info!("synced {:?} times", times,);
                        sleep(Duration::from_secs(10)).await;
                    } else {
                        bail!(anyhow!("sync_fixed_times failed"));
                    }
                }
                Err(e) => {
                    bail!(anyhow!(e));
                }
            }
        }
    }
    Ok(())
}

async fn check_new_registration(
    chain_state: Arc<ChainState>,
    signer_bls_private_key: Fr,
    next_epoch: u64,
) -> Result<()> {
    if !chain_state
        .da_signers
        .registered_epoch(chain_state.signer_address, U256::from(next_epoch))
        .call()
        .await?
    {
        info!("registering for next epoch: {:?}", next_epoch);
        let hash = epoch_registration_hash(
            chain_state.signer_address,
            next_epoch,
            chain_state.provider.get_chainid().await?.as_u64(),
        );
        let signature = (hash * signer_bls_private_key).into_affine();
        let maybe_input_data = chain_state
            .da_registry
            .register_next_epoch(serialize_g1_point(signature))
            .calldata();

        if let Some(input_data) = maybe_input_data {
            info!(
                "try to register epoch: account {:?}, epoch: {:?}",
                chain_state.signer_address, next_epoch
            );
            let tx_request = TransactionRequest::new()
                .to(chain_state.da_registry.address())
                .data(input_data);
            match chain_state
                .transactor
                .lock()
                .await
                .send(
                    tx_request,
                    TransactionInfo::RegisterEpoch(chain_state.signer_address, next_epoch),
                )
                .await
            {
                Ok(success) => {
                    if success {
                        info!("epoch {:?} registered", next_epoch);
                        metrics::REGISTERED_EPOCH.set(next_epoch as f64);
                        return Ok(());
                    }
                    bail!(anyhow!(format!("register epoch {:?} failed", next_epoch)));
                }
                Err(e) => {
                    bail!(anyhow!(e));
                }
            }
        }
    }
    Ok(())
}

async fn check_new_quorums(chain_state: Arc<ChainState>, epoch: u64) -> Result<()> {
    chain_state.fetch_quorum_if_missing(epoch).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn serialize_g1_point_test() {
        let point = G1Affine::new(
            num_bigint::BigUint::from_str(
                "6724829742155202114588703109163916832474668614509013446003558283321393615541",
            )
            .unwrap()
            .into(),
            num_bigint::BigUint::from_str(
                "18651128649493315670638266822842049221420897547844513786443506962173845494390",
            )
            .unwrap()
            .into(),
        );
        let serialized = serialize_g1_point(point);
        assert_eq!(
            serialized.x,
            U256::from_dec_str(
                "6724829742155202114588703109163916832474668614509013446003558283321393615541"
            )
            .unwrap()
        );
        assert_eq!(
            serialized.y,
            U256::from_dec_str(
                "18651128649493315670638266822842049221420897547844513786443506962173845494390"
            )
            .unwrap()
        );
    }
}
