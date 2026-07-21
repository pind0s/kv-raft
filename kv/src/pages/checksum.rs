use twox_hash::XxHash3_128;

pub(crate) fn checksum_page(data: &[u8]) -> u128 {
    XxHash3_128::oneshot(data)
}
