use std::sync::Arc;

use chain_state::transactor::{TransactionInfo, Transactor};
use chain_utils::{DefaultMiddleware, DefaultMiddlewareInner};
use contract_interface::{da_sample::SampleResponse, DASample};
use ethers::{abi::Address, types::TransactionRequest, utils::hex};
use task_executor::TaskExecutor;
use tokio::sync::{broadcast, mpsc, Mutex};

use crate::watcher::OnChainChangeMessage;

pub struct DasSubmitter {
    da_contract: DASample<DefaultMiddlewareInner>,
    on_chain_receiver: broadcast::Receiver<OnChainChangeMessage>,
    submission_receiver: mpsc::UnboundedReceiver<SampleResponse>,
    transactor: Arc<Mutex<Transactor>>,
}

impl DasSubmitter {
    pub fn spawn(
        executor: TaskExecutor,
        provider: DefaultMiddleware,
        on_chain_receiver: broadcast::Receiver<OnChainChangeMessage>,
        submission_receiver: mpsc::UnboundedReceiver<SampleResponse>,
        transactor: Arc<Mutex<Transactor>>,
        da_address: Address,
    ) {
        let da_contract = DASample::new(da_address, provider.clone());
        let submitter = Self {
            da_contract,
            submission_receiver,
            on_chain_receiver,
            transactor,
        };
        executor.spawn(
            async move { Box::pin(submitter.start()).await },
            "das_submitter",
        );
    }

    async fn start(mut self) {
        use OnChainChangeMessage::*;

        let mut enabled = true;
        let mut current_task = None;

        loop {
            tokio::select! {
                biased;

                msg = self.on_chain_receiver.recv(), if enabled => {
                    match msg {
                        Ok(NewSampleTask(task)) => {
                            current_task = Some(task);
                        },
                        Ok(ClosedSampleTask(hash)) => {
                            if current_task.map_or(false, |t| t.sample_seed == hash) {
                                current_task = None;
                            }
                        },
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Closed)=>{
                            warn!("On-chain status channel closed.");
                            self.submission_receiver.close();
                            enabled = false;
                        }
                        Err(broadcast::error::RecvError::Lagged(n))=>{
                            warn!(number = n, "On-chain status channel lagged.");
                        }
                    }
                },

                msg = self.submission_receiver.recv(), if enabled && current_task.is_some() => {
                    if msg.is_none() {
                        warn!("Submission channel closed.");
                    }

                    let response = msg.unwrap();
                    if response.sample_seed == current_task.unwrap().sample_seed.0 {
                        let _ = self.submit_response(response).await;
                    }
                }
            }
        }
    }

    async fn submit_response(&self, response: SampleResponse) -> Result<(), ()> {
        info_span!("submit_response");
        info!(
            epoch = response.epoch,
            quorum = response.quorum_id,
            data_root = hex::encode(response.data_root),
            "Start response submission"
        );

        let commitment_exists = self
            .da_contract
            .commitment_exists(
                response.data_root,
                response.epoch.into(),
                response.quorum_id.into(),
            )
            .call()
            .await
            .map_err(|e| {
                warn!(error = ?e, "Fail to check commitment exists");
            })?;

        if !commitment_exists {
            info!("Give up submission because of non-existent data root");
            return Err(());
        }

        if let Some(input_data) = self
            .da_contract
            .submit_sampling_response(response)
            .calldata()
        {
            let tx_request = TransactionRequest::new()
                .to(self.da_contract.address())
                .data(input_data);
            match self
                .transactor
                .lock()
                .await
                .send(tx_request, TransactionInfo::SubmitSamplingResponse)
                .await
            {
                Ok(success) => {
                    if success {
                        info!("Submit response success");
                    } else {
                        warn!("Submit response transaction failed");
                    }
                }
                Err(e) => {
                    warn!("Submit response failed: {:?}", e);
                }
            }
        }
        Ok(())
    }
}
