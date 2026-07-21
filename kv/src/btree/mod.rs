use crate::btree::leaf_node::LeafBuilder;
use crate::error::{Error, Result};
use zerocopy::FromBytes;
use zerocopy::byteorder::{LE, U64};

mod branch_node;
mod leaf_node;
pub(crate) mod tree;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u64)]
enum NodeKind {
    Leaf = 0,
    Branch = 1,
}

impl NodeKind {
    fn parse(page: &[u8]) -> Result<Self> {
        let (node_type, _) = U64::<LE>::ref_from_prefix(page)?;

        match node_type.get() {
            value if value == Self::Leaf as u64 => Ok(Self::Leaf),
            value if value == Self::Branch as u64 => Ok(Self::Branch),
            value => Err(Error::DatabaseError(format!("invalid node type {value}"))),
        }
    }
}

fn write_data_at_offset(page: &mut [u8], data: &[u8], offset: usize) {
    page[offset..offset + data.len()].copy_from_slice(data);
}

pub(crate) fn get_empty_leaf_page(page_size: usize) -> Vec<u8> {
    let mut page = vec![0; page_size];
    LeafBuilder::empty().build_page_at_buf(&mut page);
    page
}
