use crate::pages::PageId;
use moka::sync::SegmentedCache;
use std::sync::Arc;

// I think segmented cache is a lil faster than regular one?
// todo currently the bottleneck for read ops is cache, should look into different cache options
pub(super) struct PageCache {
    read_cache: SegmentedCache<PageId, Arc<[u8]>>,
}

impl PageCache {
    pub(super) fn new(cache_capacity: usize) -> Self {
        PageCache {
            read_cache: SegmentedCache::new(cache_capacity as u64, 4),
        }
    }

    pub(super) fn get(&self, page_id: PageId) -> Option<Arc<[u8]>> {
        self.read_cache.get(&page_id)
    }

    pub(super) fn insert(&self, page_id: PageId, data: Arc<[u8]>) {
        self.read_cache.insert(page_id, data);
    }

    pub(super) fn remove(&self, page_id: PageId) {
        self.read_cache.invalidate(&page_id);
    }
}
