use rkyv::{Archive, Deserialize, Serialize};

/// At the beginning of the file, fixed size.
#[derive(Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct FileHeader {
    pub magic_number: u32,
    pub version: u32,
    pub metadata_start_offset: u64,
    pub metadata_size: u64,
}

/// At the end of the file, variable list of pages.
#[derive(Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
pub struct Metadata<K> {
    pub page_index: Vec<PageSection<K>>,
    pub lower_bound: usize,
}

#[derive(Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
pub struct PageSection<K> {
    pub file_offset: u64,
    pub page_size: u64,
    pub range: Option<(K, K)>,
}

#[derive(Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
pub struct ColumnLayerPage<K, R> {
    pub keys: Vec<K>,
    pub diffs: Vec<R>,
}
