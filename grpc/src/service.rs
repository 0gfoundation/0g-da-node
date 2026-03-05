#![allow(unused)]

use crate::service::signer::signer_server::{Signer, SignerServer};
use crate::service::signer::{BatchSignReply, BatchSignRequest};
use anyhow::{anyhow, bail};
use ark_bn254::{Bn254, Fq, Fr, G1Affine, G1Projective};
use ark_ec::{AffineRepr, CurveGroup};
use ark_serialize::{CanonicalDeserialize, CanonicalSerialize};
use chain_state::signers_handler::serialize_g1_point;
use chain_state::ChainState;
use ethers::abi::{self, Token};
use ethers::types::{Res, U256};
use ethers::utils::keccak256;
use prost::Message;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use signer::{BatchRetrieveReply, BatchRetrieveRequest, Empty, Slices};
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage::blob_status_db::{BlobStatus, BlobStatusDB};
use storage::quorum_db::{AssignedSlices, QuorumDB};
use storage::slice_db::SliceDB;
use storage::Storage;
use tokio::sync::RwLock;
use tonic::metadata::KeyAndMutValueRef;
use tonic::{Code, Request, Response, Status};
use utils::{map_to_g1, metrics};
use zg_encoder::{DeferredVerifier, EncodedSlice, ZgEncoderParams, ZgSignerParams};

use self::signer::SignRequest;

pub mod signer {
    tonic::include_proto!("signer");
}

const DEFAULT_MAX_ONGOING_SIGN_REQUEST: u64 = 10;

pub struct SignerService {
    db: Arc<RwLock<Storage>>,
    chain_state: Arc<ChainState>,
    signer_bls_private_key: Fr,
    encoder_params: ZgSignerParams,
    max_ongoing_sign_request: u64,
    ongoing_sign_request_cnt: Arc<RwLock<u64>>,
}

impl SignerService {
    pub fn new(
        db: Arc<RwLock<Storage>>,
        chain_state: Arc<ChainState>,
        signer_bls_private_key: Fr,
        params_dir: String,
        max_ongoing_sign_request: Option<u64>,
    ) -> Self {
        Self {
            db,
            chain_state,
            signer_bls_private_key,
            encoder_params: ZgSignerParams::from_dir_mont(params_dir),
            max_ongoing_sign_request: max_ongoing_sign_request
                .unwrap_or(DEFAULT_MAX_ONGOING_SIGN_REQUEST),
            ongoing_sign_request_cnt: Arc::new(RwLock::new(0)),
        }
    }

    async fn on_incoming_batch_sign(&self) -> Result<(), Status> {
        let mut cnt = self.ongoing_sign_request_cnt.write().await;
        if *cnt > self.max_ongoing_sign_request {
            return Err(Status::new(Code::ResourceExhausted, "request pool is full"));
        }
        *cnt += 1;
        Ok(())
    }

    async fn on_complete_batch_sign(&self) {
        let mut cnt = self.ongoing_sign_request_cnt.write().await;
        *cnt -= 1;
    }

    async fn batch_sign_inner(
        &self,
        request: Request<BatchSignRequest>,
    ) -> Result<Response<BatchSignReply>, Status> {
        let remote_addr = request.remote_addr();
        let request_content = request.into_inner();
        metrics::GRPC_REQ_GAUGE.set(request_content.encoded_len() as f64);
        let ts = Instant::now();

        info!(?remote_addr, "Received sign request");
        let mut reply = BatchSignReply { signatures: vec![] };

        for req in request_content.requests.iter() {
            let (storage_root, erasure_commitment) = Self::decode_root(req)?;

            self.check_blob_status(req, storage_root).await?;

            let encoded_slices = Self::decode_encoded_slices(req)?;

            let res = self
                .verify_encoded_slices(
                    req.epoch,
                    req.quorum_id,
                    storage_root,
                    erasure_commitment,
                    &encoded_slices,
                )
                .await;

            if let Err(error) = res {
                return Err(match error {
                    VerificationError::Internal(e) => Status::new(
                        Code::Internal,
                        format!("internal error on verification: {:?}", e),
                    ),
                    VerificationError::SliceMismatch => Status::new(
                        Code::InvalidArgument,
                        "received slices and assigned slices are mismatch",
                    ),
                    VerificationError::IncorrectSlice(e) => Status::new(
                        Code::InvalidArgument,
                        format!("verification failed: {:?}", e),
                    ),
                    VerificationError::DeferredVerifyFail => Status::new(
                        Code::InvalidArgument,
                        "received slice does not pass pairing check, the accelerated verification algorithm cannot detect the specific error location".to_string(),
                    ),
                });
            }

            let hash =
                blob_verified_hash(storage_root, req.epoch, req.quorum_id, erasure_commitment);
            let signature = (hash * self.signer_bls_private_key).into_affine();
            let mut value = Vec::new();
            signature.serialize_uncompressed(&mut value);
            reply.signatures.push(value);
            // write slices to db
            self.db
                .write()
                .await
                .put_slice(req.epoch, req.quorum_id, storage_root, encoded_slices)
                .await
                .map_err(|e| Status::new(Code::Internal, format!("put slice error: {:?}", e)))?;
        }

        info!("responsed in {:?} ms", ts.elapsed().as_millis());
        Ok(Response::new(reply))
    }

    async fn batch_retrieve_inner(
        &self,
        request: Request<BatchRetrieveRequest>,
    ) -> Result<Response<BatchRetrieveReply>, Status> {
        let remote_addr = request.remote_addr();
        let request_content = request.into_inner();
        let ts = Instant::now();

        info!(?remote_addr, "Received retrieve request");
        let mut reply = BatchRetrieveReply {
            encoded_slice: vec![],
        };
        for req in request_content.requests.iter() {
            let mut slices = Slices {
                encoded_slice: vec![],
            };
            let storage_root: [u8; 32] = req
                .storage_root
                .clone()
                .try_into()
                .map_err(|_| Status::new(Code::InvalidArgument, "storage root"))?;
            let maybe_assigned_slices = self
                .db
                .read()
                .await
                .get_assgined_slices(req.epoch, req.quorum_id)
                .await
                .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
            match maybe_assigned_slices {
                Some(AssignedSlices(assigned_slices)) => {
                    let mut row_indexes = req.row_indexes.clone();
                    row_indexes.sort_unstable();
                    row_indexes.dedup();
                    if row_indexes.len() > assigned_slices.len() {
                        return Err(Status::new(Code::InvalidArgument, "invalid row indexes"));
                    }
                    let mut j = 0;
                    for row_index in row_indexes.iter() {
                        while j < assigned_slices.len() && assigned_slices[j] < *row_index as u64 {
                            j += 1;
                        }
                        if j < assigned_slices.len() && assigned_slices[j] == *row_index as u64 {
                            let maybe_slice = self
                                .db
                                .read()
                                .await
                                .get_raw_slice(
                                    req.epoch,
                                    req.quorum_id,
                                    storage_root,
                                    assigned_slices[j] as usize,
                                )
                                .await
                                .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
                            match maybe_slice {
                                Some(slice) => {
                                    slices.encoded_slice.push(slice);
                                }
                                None => {
                                    error!("slice is missing: epoch = {:?}, quorum = {:?}, storage_root = {:?}, row_index = {:?}", req.epoch, req.quorum_id, hex::encode(storage_root), assigned_slices[j]);
                                    return Err(Status::new(
                                        Code::Internal,
                                        "slice is missing".to_string(),
                                    ));
                                }
                            }
                        } else {
                            return Err(Status::new(Code::InvalidArgument, "invalid row indexes"));
                        }
                    }
                }
                None => {
                    return Err(Status::new(
                        Code::Internal,
                        format!("quorum of epoch {:?} not found", req.epoch),
                    ));
                }
            }
            reply.encoded_slice.push(slices);
        }
        Ok(Response::new(reply))
    }
}

#[tonic::async_trait]
impl Signer for SignerService {
    async fn batch_sign(
        &self,
        request: Request<BatchSignRequest>,
    ) -> Result<Response<BatchSignReply>, Status> {
        self.on_incoming_batch_sign().await?;
        metrics::GRPC_RQE_COUNTER
            .with_label_values(&["batch_sign"])
            .inc();
        let timer = metrics::GRPC_REQ_HISTOGRAM
            .with_label_values(&["batch_sign"])
            .start_timer();

        let reply = self.batch_sign_inner(request).await;
        timer.observe_duration();

        self.on_complete_batch_sign().await;
        reply
    }

    async fn batch_retrieve(
        &self,
        request: Request<BatchRetrieveRequest>,
    ) -> Result<Response<BatchRetrieveReply>, Status> {
        metrics::GRPC_RQE_COUNTER
            .with_label_values(&["batch_retrieve"])
            .inc();

        let timer = metrics::GRPC_REQ_HISTOGRAM
            .with_label_values(&["batch_retrieve"])
            .start_timer();
        let resp = self.batch_retrieve_inner(request).await;
        timer.observe_duration();
        resp
    }

    async fn get_status(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<signer::StatusReply>, Status> {
        metrics::GRPC_RQE_COUNTER
            .with_label_values(&["get_status"])
            .inc();
        let timer = metrics::GRPC_REQ_HISTOGRAM
            .with_label_values(&["get_status"])
            .start_timer();
        let status = signer::StatusReply {
            status_code: 200,
            entrance_contract: hex::encode(self.chain_state.da_entrance.address()),
        };
        timer.observe_duration();
        Ok(Response::new(status))
    }
}

pub enum VerificationError {
    Internal(anyhow::Error),
    SliceMismatch,
    IncorrectSlice(zg_encoder::VerifierError),
    DeferredVerifyFail,
}

impl From<&'static str> for VerificationError {
    fn from(error: &'static str) -> Self {
        VerificationError::Internal(anyhow!(error))
    }
}

impl From<anyhow::Error> for VerificationError {
    fn from(error: anyhow::Error) -> Self {
        VerificationError::Internal(error)
    }
}

impl From<zg_encoder::VerifierError> for VerificationError {
    fn from(error: zg_encoder::VerifierError) -> Self {
        VerificationError::IncorrectSlice(error)
    }
}

impl SignerService {
    async fn check_blob_status(
        &self,
        req: &SignRequest,
        storage_root: [u8; 32],
    ) -> Result<(), Status> {
        // --- Added Epoch Validation ---
        let current_epoch = self.chain_state.da_signers.epoch_number().call().await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?.as_u64();
        let epoch_window_size = self.chain_state.da_entrance.epoch_window_size().call().await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?.as_u64();

        if req.epoch + epoch_window_size < current_epoch {
            return Err(Status::new(Code::InvalidArgument, "epoch too old"));
        }
        // ------------------------------

        let maybe_blob_status = self
            .db
            .read()
            .await
            .get_blob_status(req.epoch, req.quorum_id, storage_root)
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        match maybe_blob_status {
            Some(BlobStatus::UPLOADED) => Ok(()),
            Some(BlobStatus::VERIFIED) => Err(Status::new(Code::Internal, "blob verified already")),
            None => Err(Status::new(Code::Internal, "blob not found")),
        }
    }

    fn decode_root(req: &SignRequest) -> Result<([u8; 32], G1Projective), Status> {
        let storage_root: [u8; 32] = req
            .storage_root
            .clone()
            .try_into()
            .map_err(|_| Status::new(Code::InvalidArgument, "storage root"))?;

        let (x, y) =
            <(Fq, Fq)>::deserialize_uncompressed(&*req.erasure_commitment).map_err(|e| {
                Status::new(
                    Code::InvalidArgument,
                    format!("failed to deserialize erasure commitment: {:?}", e),
                )
            })?;

        let maybe_commitment = G1Affine::new_unchecked(x, y);
        if !maybe_commitment.is_on_curve()
            || !maybe_commitment.is_in_correct_subgroup_assuming_on_curve()
        {
            return Err(Status::new(
                Code::InvalidArgument,
                "Incorrect commitment: commitment is not in group".to_string(),
            ));
        }

        Ok((storage_root, maybe_commitment.into_group()))
    }

    fn decode_encoded_slices(req: &SignRequest) -> Result<Vec<EncodedSlice>, Status> {
        let ts = Instant::now();
        let encoded_slices: Vec<EncodedSlice> = req
            .encoded_slice
            .par_iter()
            .map(|data| {
                EncodedSlice::deserialize_uncompressed(&*data.to_vec()).map_err(|e| {
                    Status::new(
                        Code::InvalidArgument,
                        format!("failed to deserialize slice: {:?}", e),
                    )
                })
            })
            .collect::<Result<Vec<EncodedSlice>, Status>>()?;
        info!(
            "used {:?} ms to deserialize {:?} slices.",
            ts.elapsed().as_millis(),
            encoded_slices.len()
        );
        Ok(encoded_slices)
    }

    async fn verify_encoded_slices(
        &self,
        epoch: u64,
        quorum_id: u64,
        storage_root: [u8; 32],
        erasure_commitment: G1Projective,
        encoded_slices: &Vec<EncodedSlice>,
    ) -> Result<(), VerificationError> {
        // in case quorum info is missing
        let quorum_num = self.chain_state.fetch_quorum_if_missing(epoch).await?;
        // check quorum_id
        if quorum_num <= quorum_id {
            return Err("quorum_id out of bound".into());
        }
        // check assigned slices
        let maybe_assigned_slices = self
            .db
            .read()
            .await
            .get_assgined_slices(epoch, quorum_id)
            .await?;
        match maybe_assigned_slices {
            Some(AssignedSlices(assigned_slices)) => {
                self.verify_assigned_slices(
                    storage_root,
                    erasure_commitment,
                    assigned_slices,
                    encoded_slices,
                )?;
            }
            None => {
                return Err(anyhow!("quorum of epoch {:?} not found", epoch).into());
            }
        }
        Ok(())
    }

    fn verify_assigned_slices(
        &self,
        storage_root: [u8; 32],
        erasure_commitment: G1Projective,
        assigned_slices: Vec<u64>,
        encoded_slices: &Vec<EncodedSlice>,
    ) -> Result<(), VerificationError> {
        if assigned_slices.len() != encoded_slices.len() {
            return Err(VerificationError::SliceMismatch);
        }
        let ts = Instant::now();

        let deferred_verifier = DeferredVerifier::new();
        let res: Result<(), _> = assigned_slices
            .par_iter()
            .zip(encoded_slices)
            .map(|(expected_index, slice)| {
                let verifier = deferred_verifier.clone();
                if *expected_index != slice.index as u64 {
                    Err(VerificationError::SliceMismatch)
                } else {
                    Ok(slice.verify(
                        &self.encoder_params,
                        &erasure_commitment,
                        &storage_root,
                        Some(verifier),
                    )?)
                }
            })
            .collect();

        let deferred_pass = deferred_verifier.fast_check();

        info!(
            "used {:?} ms to verify {:?} slices.",
            ts.elapsed().as_millis(),
            assigned_slices.len()
        );
        res?;

        if !deferred_pass {
            Err(VerificationError::DeferredVerifyFail)
        } else {
            Ok(())
        }
    }
}

fn u256_to_u8_array(x: U256) -> Vec<u8> {
    let mut bytes = [0; 32];
    x.to_big_endian(&mut bytes);
    bytes.to_vec()
}

pub fn blob_verified_hash(
    data_root: [u8; 32],
    epoch: u64,
    quorum_id: u64,
    erasure_commitment: G1Projective,
) -> G1Affine {
    let g1_point = serialize_g1_point(erasure_commitment.into_affine());
    let hash = keccak256(
        abi::encode_packed(&[
            Token::FixedBytes(data_root.to_vec()),
            Token::FixedBytes(u256_to_u8_array(U256::from(epoch))),
            Token::FixedBytes(u256_to_u8_array(U256::from(quorum_id))),
            Token::FixedBytes(u256_to_u8_array(g1_point.x)),
            Token::FixedBytes(u256_to_u8_array(g1_point.y)),
        ])
        .unwrap(),
    );
    map_to_g1(hash.to_vec())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use ark_bn254::g1;
    use ark_ec::AffineRepr;
    use ark_ff::Fp;
    use utils::hex_to_bytes;
    use zg_encoder::constants::G1A;

    use super::*;

    #[test]
    fn blob_verified_hash_test() {
        let a = g1::G1Affine::generator() * Fr::from(1);
        let hash = blob_verified_hash(
            hex_to_bytes("1111111111111111111111111111111111111111111111111111111111111111")
                .unwrap()
                .try_into()
                .unwrap(),
            1,
            2,
            G1Projective::new(Fp::from(1), Fp::from(2), Fp::from(1)),
        );
        assert_eq!(
            hash,
            G1Affine::new(
                num_bigint::BigUint::from_str(
                    "3104132272622526655068902279970515367044771064982988265068273751564440697689"
                )
                .unwrap()
                .into(),
                num_bigint::BigUint::from_str(
                    "14983672482514514723382346054400511740670770934276906876175822994665721348371"
                )
                .unwrap()
                .into(),
            )
        );
        let signer_bls_private_key: Fr = Fr::from_str("1").unwrap();
        let signature = (hash * signer_bls_private_key).into_affine();
        assert_eq!(
            signature,
            G1Affine::new(
                num_bigint::BigUint::from_str(
                    "3104132272622526655068902279970515367044771064982988265068273751564440697689"
                )
                .unwrap()
                .into(),
                num_bigint::BigUint::from_str(
                    "14983672482514514723382346054400511740670770934276906876175822994665721348371"
                )
                .unwrap()
                .into(),
            )
        );
    }
}
