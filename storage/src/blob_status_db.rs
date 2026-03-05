use crate::COL_BLOB_STATUS;

use super::Storage;
use anyhow::{anyhow, Result};
use async_trait::async_trait;

use std::convert::TryFrom;

pub enum BlobStatus {
    UPLOADED = 1,
    VERIFIED = 2,
}

impl TryFrom<u64> for BlobStatus {
    type Error = ();

    fn try_from(v: u64) -> Result<Self, Self::Error> {
        match v {
            x if x == BlobStatus::UPLOADED as u64 => Ok(BlobStatus::UPLOADED),
            x if x == BlobStatus::VERIFIED as u64 => Ok(BlobStatus::VERIFIED),
            _ => Err(()),
        }
    }
}

#[async_trait]
pub trait BlobStatusDB {
    async fn put_blob(
        &self,
        epoch: u64,
        quorum_id: u64,
        storage_root: [u8; 32],
        status: BlobStatus,
    ) -> Result<()>;
    async fn get_blob_status(
        &self,
        epoch: u64,
        quorum_id: u64,
        storage_root: [u8; 32],
    ) -> Result<Option<BlobStatus>>;
    // Added prune method to trait
    async fn prune(&self, epoch: u64) -> Result<()>;
}

fn get_blob_key(epoch: u64, quorum_id: u64, storage_root: [u8; 32]) -> Vec<u8> {
    epoch
        .to_be_bytes()
        .into_iter()
        .chain(quorum_id.to_be_bytes())
        .chain(storage_root)
        .collect()
}

#[async_trait]
impl BlobStatusDB for Storage {
    async fn put_blob(
        &self,
        epoch: u64,
        quorum_id: u64,
        storage_root: [u8; 32],
        status: BlobStatus,
    ) -> Result<()> {
        let key = get_blob_key(epoch, quorum_id, storage_root);
        let mut tx = self.db.transaction();
        tx.put(COL_BLOB_STATUS, &key, &(status as u64).to_be_bytes());
        self.db.write(tx)?;
        Ok(())
    }

    async fn get_blob_status(
        &self,
        epoch: u64,
        quorum_id: u64,
        storage_root: [u8; 32],
    ) -> Result<Option<BlobStatus>> {
        let key = get_blob_key(epoch, quorum_id, storage_root);
        if let Some(raw_data) = self.db.get(COL_BLOB_STATUS, &key)? {
            let status: BlobStatus = u64::from_be_bytes(raw_data.try_into().unwrap())
                .try_into()
                .map_err(|_| anyhow!("error when convert u64 to BlobStatus"))?;
            return Ok(Some(status));
        }
        Ok(None)
    }

    // Added implementation of prune
    async fn prune(&self, epoch: u64) -> Result<()> {
        let prefix: Vec<u8> = epoch.to_be_bytes().to_vec();
        let mut tx = self.db.transaction();
        tx.delete_prefix(COL_BLOB_STATUS, &prefix);
        self.db.write(tx)?;
        Ok(())
    }
}
