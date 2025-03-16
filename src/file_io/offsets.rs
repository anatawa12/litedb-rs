pub mod index_node {
    use crate::utils::PageAddress;

    pub const P_SLOT: usize = 0; // 00-00 [byte]
    pub const P_LEVELS: usize = 1; // 01-01 [byte]
    pub const P_DATA_BLOCK: usize = 2; // 02-06 [PageAddress]
    pub const P_NEXT_NODE: usize = 7; // 07-11 [PageAddress]
    pub const P_PREV_NEXT: usize = 12; // 12-(_level * 5 [PageAddress] * 2 [prev-next])

    pub fn calc_key_ptr(levels: u8) -> usize {
        P_PREV_NEXT + levels as usize * PageAddress::SERIALIZED_SIZE * 2
    }
}

pub mod data_block {
    pub const P_EXTEND: usize = 0; // 00-00 [byte]
    pub const P_NEXT_BLOCK: usize = 1; // 01-05 [pageAddress]
    pub const P_BUFFER: usize = 6; // 06-EOF [byte[]]
}

pub mod header_page {
    pub const HEADER_INFO: &[u8] = b"** This is a LiteDB file **";
    pub const FILE_VERSION: u8 = 8;

    pub const P_HEADER_INFO: usize = 32; // 32-58 (27 bytes)
    pub const P_FILE_VERSION: usize = 59; // 59-59 (1 byte)
    pub const P_FREE_EMPTY_PAGE_ID: usize = 60; // 60-63 (4 bytes)
    pub const P_LAST_PAGE_ID: usize = 64; // 64-67 (4 bytes)
    pub const P_CREATION_TIME: usize = 68; // 68-75 (8 bytes)

    //pub const P_PRAGMAS: usize = 76; // 76-190 (115 bytes)
    #[allow(dead_code)] // no rebuild is supported (for now)
    pub const P_INVALID_DATAFILE_STATE: usize = 191; // 191-191 (1 byte)

    pub const P_COLLECTIONS: usize = 192; // 192-8159 (8064 bytes)
    pub const COLLECTIONS_SIZE: usize = 8000; // 250 blocks with 32 bytes each
}

pub mod collection_page {
    use crate::constants::PAGE_SIZE;

    pub const P_INDEXES: usize = 96; // 96-8192 (64 + 32 header = 96)
    #[allow(dead_code)]
    pub const P_INDEXES_COUNT: usize = PAGE_SIZE - P_INDEXES;
}
