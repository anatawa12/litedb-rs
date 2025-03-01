use crate::Result;
use crate::engine::buffer_reader::BufferReader;
use crate::engine::buffer_writer::BufferWriter;
use crate::engine::page_address::PageAddress;
use crate::expression::BsonExpression;

pub(crate) struct CollectionIndex {
    slot: u8,
    index_type: u8,
    name: String,
    expression: String,
    // bson_expr:
    unique: bool,
    head: PageAddress,
    tail: PageAddress,
    reserved: u8, // previously level max
    free_index_page_list: u32,
    bson_expr: BsonExpression,
}

impl CollectionIndex {
    pub fn new(
        slot: u8,
        index_type: u8,
        name: String,
        expression: BsonExpression,
        unique: bool,
    ) -> Self {
        Self {
            slot,
            index_type,
            name,
            expression: expression.source().to_string(),
            unique,
            head: PageAddress::EMPTY,
            tail: PageAddress::EMPTY,
            reserved: 0,
            free_index_page_list: u32::MAX,
            bson_expr: expression,
        }
    }

    pub fn load(reader: &mut BufferReader) -> Result<Self> {
        let slot = reader.read_u8();
        let index_type = reader.read_u8();
        let name = reader
            .read_cstring()
            .ok_or_else(crate::Error::invalid_page)?;
        let expression = reader
            .read_cstring()
            .ok_or_else(crate::Error::invalid_page)?;
        let unique = reader.read_bool();
        let head = reader.read_page_address();
        let tail = reader.read_page_address();
        let reserved = reader.read_u8();
        let free_index_page_list = reader.read_u32();
        let parsed = BsonExpression::create(&expression)?;

        Ok(Self {
            slot,
            index_type,
            name,
            expression,
            unique,
            head,
            tail,
            reserved,
            free_index_page_list,
            bson_expr: parsed,
        })
    }

    pub fn update_buffer(&self, writer: &mut BufferWriter) {
        writer.write_u8(self.slot);
        writer.write_u8(self.index_type);
        writer.write_cstring(&self.name);
        writer.write_cstring(&self.expression);
        writer.write_bool(self.unique);
        writer.write_page_address(self.head);
        writer.write_page_address(self.tail);
        writer.write_u8(self.reserved);
        writer.write_u32(self.free_index_page_list);
    }

    pub fn slot(&self) -> u8 {
        self.slot
    }

    #[allow(dead_code)] // reserved
    pub fn index_type(&self) -> u8 {
        self.index_type
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn expression(&self) -> &str {
        &self.expression
    }

    pub fn bson_expr(&self) -> &BsonExpression {
        &self.bson_expr
    }

    pub fn unique(&self) -> bool {
        self.unique
    }

    pub fn head(&self) -> PageAddress {
        self.head
    }

    pub fn set_head(&mut self, page: PageAddress) {
        self.head = page;
    }

    pub fn tail(&self) -> PageAddress {
        self.tail
    }

    pub fn set_tail(&mut self, page: PageAddress) {
        self.tail = page;
    }

    #[allow(dead_code)] // reserved
    pub fn reserved(&self) -> u8 {
        self.reserved
    }

    pub fn free_index_page_list(&self) -> u32 {
        self.free_index_page_list
    }

    pub fn free_index_page_list_mut(&mut self) -> &mut u32 {
        &mut self.free_index_page_list
    }

    pub fn set_free_index_page_list(&mut self, list: u32) {
        self.free_index_page_list = list;
    }

    pub fn get_length(&self) -> usize {
        Self::get_length_static(&self.name, &self.expression)
    }

    pub fn get_length_static(name: &str, expr: &str) -> usize {
        1 + 1
            + name.len()
            + 1
            + expr.len()
            + 1
            + 1
            + PageAddress::SERIALIZED_SIZE
            + PageAddress::SERIALIZED_SIZE
            + 1
            + 4
    }
}
