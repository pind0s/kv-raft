use crate::pages::PageId;
use std::sync::Arc;

pub(crate) struct Page {
    id: PageId,
    data: Arc<[u8]>,
}

pub(crate) struct PageMut {
    id: PageId,
    data: Vec<u8>,
}

impl Page {
    pub(crate) fn new(id: PageId, data: Arc<[u8]>) -> Self {
        Page { id, data }
    }

    pub(crate) fn data(&self) -> &[u8] {
        &self.data
    }

    pub(crate) fn into_arc(self) -> Arc<[u8]> {
        self.data
    }

    pub(crate) fn id(&self) -> PageId {
        self.id
    }
}

impl PageMut {
    pub(crate) fn zeroed(id: PageId, page_size: usize) -> Self {
        PageMut {
            id,
            data: vec![0; page_size],
        }
    }

    pub(crate) fn data(&self) -> &[u8] {
        &self.data
    }

    pub(crate) fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    pub(crate) fn freeze(self) -> Page {
        Page {
            id: self.id,
            data: self.data.into(),
        }
    }

    pub(crate) fn id(&self) -> PageId {
        self.id
    }
}
