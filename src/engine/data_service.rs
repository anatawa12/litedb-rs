use crate::engine::data_block::{DataBlock, DataBlockMut};
use crate::engine::snapshot::SnapshotDataPages;
use crate::engine::utils::{PartialBorrower, PartialRefMut};
use crate::engine::{
    BasePage, BufferWriter, MAX_DOCUMENT_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE, PageAddress,
};
use crate::{Error, Result, bson};
use std::cmp::min;

pub(crate) struct DataService<'a> {
    data_blocks: PartialDataBlockAccessorMut<'a>,
    max_item_count: u32,
}

impl<'a> DataService<'a> {
    pub const MAX_DATA_BYTES_PER_PAGE: usize =
        PAGE_SIZE - PAGE_HEADER_SIZE - BasePage::SLOT_SIZE - DataBlock::DATA_BLOCK_FIXED_SIZE;

    pub fn new(data_blocks: SnapshotDataPages<'a>, max_item_count: u32) -> Self {
        Self {
            data_blocks: PartialDataBlockAccessorMut::new(data_blocks),
            max_item_count,
        }
    }

    pub async fn insert(&mut self, value: &bson::Document) -> Result<PageAddress> {
        let mut bytes_left = value.get_serialized_value_len();
        if bytes_left > MAX_DOCUMENT_SIZE {
            return Err(Error::document_size_exceed_limit());
        }

        let mut first_block = PageAddress::EMPTY;

        let mut buffers = Vec::<DataBlockMutRef>::new();
        {
            let mut block_index = 0;

            while bytes_left > 0 {
                let bytes_to_copy = min(bytes_left, Self::MAX_DATA_BYTES_PER_PAGE);
                let data_block = self
                    .data_blocks
                    .insert_data_block(bytes_to_copy, block_index > 0)
                    .await?;
                block_index += 1;

                if let Some(ref mut last_block) = buffers.last_mut() {
                    last_block.set_next_block(data_block.position());
                }

                if first_block.is_empty() {
                    first_block = data_block.position();
                }

                self.data_blocks
                    .snapshot_mut()
                    .add_or_remove_free_data_list(data_block.position().page_id())
                    .await?;

                buffers.push(data_block);

                bytes_left -= bytes_to_copy;
            }
        }

        let buffers = buffers
            .iter_mut()
            .map(|x| x.buffer_mut())
            .collect::<Vec<_>>();

        let mut writer = BufferWriter::fragmented(buffers);
        writer.write_document(value);

        Ok(first_block)
    }

    pub async fn update(
        &mut self,
        block_address: PageAddress,
        doc: &bson::Document,
    ) -> std::result::Result<(), Error> {
        let mut bytes_left = doc.get_serialized_value_len();
        if bytes_left > MAX_DOCUMENT_SIZE {
            return Err(Error::document_size_exceed_limit());
        }

        let mut buffers = Vec::<DataBlockMutRef>::new();

        {
            let mut update_address = block_address;

            while bytes_left > 0 {
                let bytes_to_copy;
                // if last block contains new block sequence, continue updating
                if !update_address.is_empty() {
                    let mut current_block = self.data_blocks.get_block_mut(update_address).await?;

                    // TODO(rust): due to implementation limitation, we removed extending existing blocks
                    // try get full page size content (do not add DATA_BLOCK_FIXED_SIZE because will be added in UpdateBlock)
                    //bytes_to_copy = min(bytes_left, dataPage.FreeBytes + current_block.Buffer.Count);
                    //let mut update_block = dataPage.UpdateBlock(current_block, bytes_to_copy);
                    bytes_to_copy = current_block.buffer_mut().len();
                    let update_block = current_block;

                    self.data_blocks
                        .snapshot_mut()
                        .add_or_remove_free_data_list(update_block.position().page_id())
                        .await?;

                    // go to next address (if exists)
                    update_address = update_block.next_block();

                    buffers.push(update_block);
                } else {
                    bytes_to_copy = min(bytes_left, DataService::MAX_DATA_BYTES_PER_PAGE);
                    let insert_block = self
                        .data_blocks
                        .insert_data_block(bytes_to_copy, true)
                        .await?;

                    if let Some(last_block) = buffers.last_mut() {
                        last_block.set_next_block(insert_block.position());
                    }

                    self.data_blocks
                        .snapshot_mut()
                        .add_or_remove_free_data_list(insert_block.position().page_id())
                        .await?;

                    buffers.push(insert_block);
                }

                bytes_left -= bytes_to_copy;
            }

            // old document was bigger than current, must delete extend blocks
            if let Some(last_block) = buffers.last_mut() {
                if !last_block.next_block().is_empty() {
                    let next_block_address = last_block.next_block();

                    last_block.set_next_block(PageAddress::EMPTY);

                    self.delete(next_block_address).await?;
                }
            }
        }

        let buffers = buffers
            .iter_mut()
            .map(|x| x.buffer_mut())
            .collect::<Vec<_>>();
        let mut writer = BufferWriter::fragmented(buffers);
        writer.write_document(doc);

        Ok(())
    }

    pub async fn read(&mut self, mut address: PageAddress) -> Result<Vec<DataBlockMutRef<'a>>> {
        let mut buffer = vec![];
        // recursive check with accessor
        //let mut counter = 0;

        while !address.is_empty() {
            //debug_assert!(counter++ < _maxItemsCount, "Detected loop in data Read({0})", address);

            let block = self.data_blocks.get_block_mut(address).await?;

            address = block.next_block();
            buffer.push(block);
        }

        Ok(buffer)
    }

    pub async fn delete(&mut self, mut block_address: PageAddress) -> Result<()> {
        while !block_address.is_empty() {
            let next_block = self.data_blocks.delete_block(block_address).await?;

            // fix page empty list (or delete page)
            self.data_blocks
                .snapshot_mut()
                .add_or_remove_free_data_list(block_address.page_id())
                .await?;

            block_address = next_block;
        }

        Ok(())
    }
}

pub(crate) struct PartialDataBlockAccessorMut<'snapshot> {
    inner: PartialBorrower<SnapshotDataPages<'snapshot>, PageAddress>,
}

type DataBlockMutRef<'snapshot> = PartialRefMut<DataBlockMut<'snapshot>, PageAddress>;

impl<'snapshot> PartialDataBlockAccessorMut<'snapshot> {
    pub(crate) fn new(snapshot: SnapshotDataPages<'snapshot>) -> Self {
        Self {
            inner: PartialBorrower::new(snapshot),
        }
    }

    fn snapshot_mut(&mut self) -> &mut SnapshotDataPages<'snapshot> {
        self.inner.target_mut()
    }

    async fn insert_data_block(
        &mut self,
        length: usize,
        extend: bool,
    ) -> Result<DataBlockMutRef<'snapshot>> {
        unsafe {
            self.inner
                .try_create_borrow_async(
                    async |snapshot: &mut SnapshotDataPages<'snapshot>| {
                        Ok(snapshot
                            .get_free_data_page(length)
                            .await?
                            .get_mut()
                            .insert_block(length, extend))
                    },
                    |s| s.position(),
                )
                .await
        }
    }

    async fn get_block_mut(&mut self, address: PageAddress) -> Result<DataBlockMutRef<'snapshot>> {
        Ok(self.get_node_mut_opt(address).await?.expect("not found"))
    }

    async fn get_node_mut_opt(
        &mut self,
        address: PageAddress,
    ) -> Result<Option<DataBlockMutRef<'snapshot>>> {
        if address.page_id() == u32::MAX {
            return Ok(None);
        }

        unsafe {
            Ok(Some(
                self.inner
                    .try_get_borrow_async::<_, _, Error>(
                        address,
                        async |snapshot: &mut SnapshotDataPages, address| {
                            Ok(snapshot
                                .get_page(address.page_id())
                                .await?
                                .get_mut()
                                .get_data_block_mut(address.index()))
                        },
                    )
                    .await?,
            ))
        }
    }

    // returns next block address
    async fn delete_block(&mut self, address: PageAddress) -> Result<PageAddress> {
        unsafe {
            self.inner
                .try_delete_borrow_async(
                    address,
                    async |snapshot: &mut SnapshotDataPages, address| {
                        let block = snapshot.get_page(address.page_id()).await?.get_mut();

                        let next_block = block.get_data_block(address.index()).next_block();
                        block.delete_block(address.index());
                        Ok(next_block)
                    },
                )
                .await
        }
    }
}
