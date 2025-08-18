use std::sync::Arc;

use chain_state::transactor::Transactor;
use chain_utils::DefaultMiddleware;
use contract_interface::da_sample::SampleResponse;
use ethers::types::Address;
use storage::Storage;
use task_executor::TaskExecutor;
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};

use crate::{
    line_candidate::LineCandidate, mock_data::store_mock_data, stage1::DasStage1Miner,
    stage2::DasStage2Miner, submitter::DasSubmitter, watcher::DasWatcher,
};

pub struct DasMineService;

impl DasMineService {
    pub async fn spawn(
        executor: TaskExecutor,
        provider: DefaultMiddleware,
        da_address: Address,
        das_test: bool,
        store: Arc<RwLock<Storage>>,
        transactor: Arc<Mutex<Transactor>>,
    ) -> Result<(), String> {
        info_span!("start_mine_service");

        if das_test {
            info!("Start store mock da data");
            store_mock_data("./params", &*store.read().await).await;
        }

        let (on_chain_sender, on_chain_receiver) = broadcast::channel(1024);

        let (first_stage_sender, first_stage_receiver) =
            mpsc::unbounded_channel::<Vec<LineCandidate>>();
        let (submission_sender, submission_receiver) = mpsc::unbounded_channel::<SampleResponse>();

        DasWatcher::spawn(
            executor.clone(),
            provider.clone(),
            on_chain_sender,
            da_address,
        )
        .await?;

        DasStage1Miner::spawn(
            executor.clone(),
            store.clone(),
            on_chain_receiver.resubscribe(),
            first_stage_sender,
        );

        DasStage2Miner::spawn(
            executor.clone(),
            store.clone(),
            first_stage_receiver,
            submission_sender,
        );

        DasSubmitter::spawn(
            executor.clone(),
            provider.clone(),
            on_chain_receiver.resubscribe(),
            submission_receiver,
            transactor,
            da_address,
        );

        Ok(())
    }
}
