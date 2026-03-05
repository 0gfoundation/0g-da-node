use std::{collections::BTreeSet, iter::once};

// Added COL_BLOB_STATUS here
use crate::{COL_SLICE, COL_BLOB_STATUS};

use super::Storage;
use anyhow::{bail, Result};
use ark_serialize::{CanonicalDeserialize, CanonicalSerialize};
use async_trait::async_trait;
use kvdb::KeyValueDB;
use zg_encoder::{EncodedSlice, LightEncodedSlice};

#[derive(Debug, PartialEq, Eq)]
pub struct SliceIndex {
    pub epoch: u64,
    pub quorum_id: u64,
    pub storage_root: [u8; 32],
    pub index: u64,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlobInfo {
    pub quorum_id: u64,
    pub storage_root: [u8; 32],
    pub indicies: Vec<u16>,
}

const BLOB_PREFIX: u8 = 0;
const SLICE_PREFIX: u8 = 1;
const DATA_PREFIX: u8 = 2;

impl SliceIndex {
    fn to_slice_key(&self) -> Vec<u8> {
        self.to_key_with_prefix(SLICE_PREFIX)
    }
    fn to_data_key(&self) -> Vec<u8> {
        self.to_key_with_prefix(DATA_PREFIX)
    }

    fn to_key_with_prefix(&self, prefix: u8) -> Vec<u8> {
        once(prefix)
            .chain(self.epoch.to_be_bytes())
            .chain(self.quorum_id.to_be_bytes())
            .chain(self.storage_root)
            .chain(self.index.to_be_bytes())
            .collect()
    }
}

#[async_trait]
pub trait SliceDB {
    async fn get_raw_slice(
        &self,
        epoch: u64,
        quorum_id: u64,
        storage_root: [u8; 32],
        index: usize,
    ) -> Result<Option<Vec<u8>>>;

    async fn get_slice_data(
        &self,
        epoch: u64,
        quorum_id: u64,
        storage_root: [u8; 32],
        index: usize,
    ) -> Result<Option<Vec<[u8; 32]>>>;

    async fn get_slice(
        &self,
        epoch: u64,
        quorum_id: u64,
        storage_root: [u8; 32],
        index: usize,
    ) -> Result<Option<LightEncodedSlice>>;

    async fn put_slice(
        &self,
        epoch: u64,
        quorum_id: u64,
        storage_root: [u8; 32],
        slices: Vec<EncodedSlice>,
    ) -> Result<()>;

    async fn get_epoch_info(&self, epoch: u64) -> Result<BTreeSet<BlobInfo>>;

    async fn prune(&self, epoch: u64) -> Result<()>;
}

#[async_trait]
impl SliceDB for Storage {
    async fn get_raw_slice(
        &self,
        epoch: u64,
        quorum_id: u64,
        storage_root: [u8; 32],
        index: usize,
    ) -> Result<Option<Vec<u8>>> {
        let index = SliceIndex {
            epoch,
            quorum_id,
            storage_root,
            index: index as u64,
        };
        if let Some(slice) = self.db.get(COL_SLICE, &index.to_data_key())? {
            Ok(Some(slice))
        } else {
            Ok(None)
        }
    }

    async fn get_slice_data(
        &self,
        epoch: u64,
        quorum_id: u64,
        storage_root: [u8; 32],
        index: usize,
    ) -> Result<Option<Vec<[u8; 32]>>> {
        let raw_slice = if let Some(slice) = self
            .get_raw_slice(epoch, quorum_id, storage_root, index)
            .await?
        {
            slice
        } else {
            return Ok(None);
        };
        Ok(Some(
            CanonicalDeserialize::deserialize_uncompressed_unchecked(&*raw_slice)?,
        ))
    }

    async fn get_slice(
        &self,
        epoch: u64,
        quorum_id: u64,
        storage_root: [u8; 32],
        index: usize,
    ) -> Result<Option<LightEncodedSlice>> {
        let index = SliceIndex {
            epoch,
            quorum_id,
            storage_root,
            index: index as u64,
        };
        let raw_slice = if let Some(slice) = self.db.get(COL_SLICE, &index.to_slice_key())? {
            slice
        } else {
            return Ok(None);
        };
        let check = if cfg!(test) {
            ark_serialize::Validate::Yes
        } else {
            ark_serialize::Validate::No
        };
        // Note: Slice is stored in compressed form
        let slice = LightEncodedSlice::deserialize_with_mode(
            &*raw_slice,
            ark_serialize::Compress::Yes,
            check,
        )?;
        Ok(Some(slice))
    }

    async fn put_slice(
        &self,
        epoch: u64,
        quorum_id: u64,
        storage_root: [u8; 32],
        slices: Vec<EncodedSlice>,
    ) -> Result<()> {
        let mut tx = self.db.transaction();

        let blob_key: Vec<u8> = once(BLOB_PREFIX)
            .chain(epoch.to_be_bytes())
            .chain(quorum_id.to_be_bytes())
            .chain(storage_root)
            .collect();

        // TODO: should we consider the update logic here?
        let indicies: Vec<u16> = slices.iter().map(|slice| slice.index as u16).collect();
        tx.put(COL_SLICE, &blob_key, &bcs::to_bytes(&indicies).unwrap());

        for slice in slices.into_iter() {
            let index = SliceIndex {
                epoch,
                quorum_id,
                storage_root,
                index: slice.index as u64,
            };
            let data = slice.merkle_row();
            let light_slice = slice.into_light_slice();

            let mut value: Vec<u8> = Vec::new();
            // Note: Slice is stored in compressed form
            light_slice.serialize_compressed(&mut value).unwrap();
            tx.put(COL_SLICE, &index.to_slice_key(), &value);

            let mut value: Vec<u8> = Vec::new();
            data.serialize_uncompressed(&mut value).unwrap();
            tx.put(COL_SLICE, &index.to_data_key(), &value);
        }

        self.db.write(tx)?;
        Ok(())
    }

    async fn get_epoch_info(&self, epoch: u64) -> Result<BTreeSet<BlobInfo>> {
        let prefix: Vec<u8> = once(BLOB_PREFIX).chain(epoch.to_be_bytes()).collect();

        let mut answer = BTreeSet::new();

        for item in KeyValueDB::iter_with_prefix(&*self.db, COL_SLICE, &prefix) {
            let (key, value) = item?;
            if key.len() != 1 + 8 + 8 + 32 {
                bail!("Incorrect key format");
            }
            let mut key_slice = &key.as_ref()[9..];

            let quorum_id = {
                let (cur, rest) = key_slice.split_first_chunk::<8>().unwrap();
                key_slice = rest;
                u64::from_be_bytes(*cur)
            };

            let storage_root = {
                let (cur, rest) = key_slice.split_first_chunk::<32>().unwrap();
                assert!(rest.is_empty());
                *cur
            };

            let indicies: Vec<u16> = bcs::from_bytes(&value)?;
            answer.insert(BlobInfo {
                quorum_id,
                storage_root,
                indicies,
            });
        }

        Ok(answer)
    }

    async fn prune(&self, epoch: u64) -> Result<()> {
        let blob_prefix: Vec<u8> = once(BLOB_PREFIX).chain(epoch.to_be_bytes()).collect();
        let slice_prefix: Vec<u8> = once(SLICE_PREFIX).chain(epoch.to_be_bytes()).collect();
        let data_prefix: Vec<u8> = once(DATA_PREFIX).chain(epoch.to_be_bytes()).collect();
        
        // Added this line to create the status prefix
        let blob_status_prefix: Vec<u8> = epoch.to_be_bytes().to_vec();

        let mut tx = self.db.transaction();
        tx.delete_prefix(COL_SLICE, &blob_prefix);
        tx.delete_prefix(COL_SLICE, &slice_prefix);
        tx.delete_prefix(COL_SLICE, &data_prefix);
        
        // Added this line to prune COL_BLOB_STATUS
        tx.delete_prefix(COL_BLOB_STATUS, &blob_status_prefix);

        self.db.write(tx)?;
        Ok(())
    }
}
