use crate::ffi::{duckdb_create_selection_vector, duckdb_selection_vector, duckdb_selection_vector_get_data_ptr};
use libduckdb_sys::idx_t;
use std::ptr;

pub struct SelectionVector {
    ptr: duckdb_selection_vector,
    len: idx_t,
}

impl SelectionVector {
    pub fn new_copy(vec: &[u32]) -> Self {
        let ptr = unsafe { duckdb_create_selection_vector(vec.len() as idx_t) };

        let data = unsafe { duckdb_selection_vector_get_data_ptr(ptr) };
        unsafe {
            ptr::copy_nonoverlapping(vec.as_ptr(), data, vec.len());
        }
        Self {
            ptr,
            len: vec.len() as idx_t,
        }
    }

    pub(crate) fn as_ptr(&self) -> duckdb_selection_vector {
        self.ptr
    }

    pub fn len(&self) -> u64 {
        self.len
    }
}

impl FromIterator<u32> for SelectionVector {
    fn from_iter<T: IntoIterator<Item = u32>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();

        // We only support creation of a sel vector from a sized iterator.
        assert_eq!(Some(lower), upper);

        let len = lower;
        let ptr = unsafe { duckdb_create_selection_vector(len as idx_t) };
        let mut data = unsafe { duckdb_selection_vector_get_data_ptr(ptr) };
        let hd = data;

        iter.for_each(|item| unsafe {
            // SAFETY: We know we have enough capacity to write the item.
            data.write(item);
            data = data.add(1);
            debug_assert!(data < hd.add(len));
        });

        SelectionVector { ptr, len: len as idx_t }
    }
}

#[cfg(test)]
mod tests {
    use crate::core::SelectionVector;

    #[test]
    fn test_selection_vector() {
        let vec: SelectionVector = (0..2048).collect();
        assert_eq!(vec.len(), 2048);
    }

    #[test]
    fn test_large_selection_vector() {
        let vec: SelectionVector = (0..2049).collect();
        assert_eq!(vec.len(), 2049);
    }
}
