use super::{LogicalTypeHandle, Value};
use crate::{
    core::selection_vector::SelectionVector,
    ffi::{
        duckdb_create_vector, duckdb_destroy_vector, duckdb_list_entry, duckdb_list_vector_get_child,
        duckdb_list_vector_get_size, duckdb_list_vector_reserve, duckdb_list_vector_set_size,
        duckdb_set_dictionary_vector_id, duckdb_slice_vector, duckdb_struct_type_child_count,
        duckdb_struct_type_child_name, duckdb_struct_vector_get_child, duckdb_validity_set_row_invalid, duckdb_vector,
        duckdb_vector_assign_string_element, duckdb_vector_assign_string_element_len,
        duckdb_vector_ensure_validity_writable, duckdb_vector_get_column_type, duckdb_vector_get_data,
        duckdb_vector_get_validity, duckdb_vector_reference_value, duckdb_vector_reference_vector, duckdb_vector_size,
    },
};
use libduckdb_sys::{
    duckdb_array_type_array_size, duckdb_array_vector_get_child, duckdb_validity_row_is_valid, DuckDbString,
};
use std::{any::Any, ffi::CString, slice};

/// Vector trait.
pub trait Vector {
    /// Returns a reference to the underlying Any type that this trait object
    fn as_any(&self) -> &dyn Any;
    /// Returns a mutable reference to the underlying Any type that this trait object
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

/// A flat vector
pub struct FlatVector {
    ptr: duckdb_vector,
    capacity: usize,
    owned: bool,
}

impl Clone for FlatVector {
    fn clone(&self) -> Self {
        Self {
            ptr: self.ptr,
            capacity: self.capacity,
            owned: false,
        }
    }
}

impl From<duckdb_vector> for FlatVector {
    fn from(ptr: duckdb_vector) -> Self {
        Self {
            ptr,
            capacity: unsafe { duckdb_vector_size() as usize },
            owned: false,
        }
    }
}

impl Vector for FlatVector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
}

impl Drop for FlatVector {
    fn drop(&mut self) {
        if self.owned && !self.ptr.is_null() {
            unsafe { duckdb_destroy_vector(&mut self.ptr) }
        }
    }
}

impl FlatVector {
    fn with_capacity(ptr: duckdb_vector, capacity: usize) -> Self {
        Self {
            ptr,
            capacity,
            owned: false,
        }
    }

    pub fn allocate_new_vector_with_capacity(logical_type: LogicalTypeHandle, capacity: usize) -> Self {
        let ptr = unsafe { duckdb_create_vector(logical_type.ptr, capacity as u64) };
        Self {
            ptr,
            capacity,
            owned: true,
        }
    }

    /// Returns the capacity of the vector
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns true if the row at the given index is null
    pub fn row_is_null(&self, row: u64) -> bool {
        // use idx_t entry_idx = row_idx / 64; idx_t idx_in_entry = row_idx % 64; bool is_valid = validity_mask[entry_idx] & (1 « idx_in_entry);
        // as the row is valid function is slower
        let valid = unsafe {
            let validity = duckdb_vector_get_validity(self.ptr);

            // validity can return a NULL pointer if the entire vector is valid
            if validity.is_null() {
                return false;
            }

            duckdb_validity_row_is_valid(validity, row)
        };

        !valid
    }

    /// Returns an unsafe mutable pointer to the vector’s
    pub fn as_mut_ptr<T>(&self) -> *mut T {
        unsafe { duckdb_vector_get_data(self.ptr).cast() }
    }

    /// Returns a slice of the vector
    pub fn as_slice<T>(&self) -> &[T] {
        unsafe { slice::from_raw_parts(self.as_mut_ptr(), self.capacity()) }
    }

    /// Returns a slice of the vector up to a certain length
    pub fn as_slice_with_len<T>(&self, len: usize) -> &[T] {
        unsafe { slice::from_raw_parts(self.as_mut_ptr(), len) }
    }

    /// Returns a mutable slice of the vector
    pub fn as_mut_slice<T>(&mut self) -> &mut [T] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), self.capacity()) }
    }

    /// Returns a mutable slice of the vector up to a certain length
    pub fn as_mut_slice_with_len<T>(&mut self, len: usize) -> &mut [T] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), len) }
    }

    /// Returns the logical type of the vector
    pub fn logical_type(&self) -> LogicalTypeHandle {
        unsafe { LogicalTypeHandle::new(duckdb_vector_get_column_type(self.ptr)) }
    }

    /// Returns the validity mask of the vector, if one is allocated.
    pub fn validity_slice(&self) -> Option<&mut [u64]> {
        unsafe { duckdb_vector_get_validity(self.ptr).as_mut() }
            .map(|ptr| unsafe { slice::from_raw_parts_mut(ptr, self.capacity().div_ceil(64)) })
    }

    pub fn init_get_validity_slice(&self) -> &mut [u64] {
        unsafe { duckdb_vector_ensure_validity_writable(self.ptr) };
        self.validity_slice().expect("validity slice should be initialized")
    }

    /// Set row as null
    pub fn set_null(&mut self, row: usize) {
        unsafe {
            duckdb_vector_ensure_validity_writable(self.ptr);
            let idx = duckdb_vector_get_validity(self.ptr);
            duckdb_validity_set_row_invalid(idx, row as u64);
        }
    }

    pub fn slice(&mut self, dict_len: u64, selection_vector: SelectionVector) -> DictionaryVector {
        unsafe { duckdb_slice_vector(self.ptr, dict_len, selection_vector.as_ptr(), selection_vector.len()) }
        DictionaryVector::from(self.ptr)
    }

    pub fn set_dictionary_id(&mut self, dict_id: String) {
        let dict_id = CString::new(dict_id).expect("CString::new failed");
        unsafe {
            duckdb_set_dictionary_vector_id(self.ptr, dict_id.as_ptr(), dict_id.as_bytes().len().try_into().unwrap())
        }
        std::mem::forget(dict_id);
    }

    pub fn assign_to_constant(&mut self, value: &Value) {
        // Copies value internally
        unsafe { duckdb_vector_reference_value(self.ptr, value.ptr) }
        // Sets the internal duckdb buffer to be of size 1
        self.capacity = 1;
    }

    pub fn reference(&mut self, other: &FlatVector) {
        unsafe { duckdb_vector_reference_vector(self.ptr, other.ptr) }
        self.capacity = other.capacity;
    }

    /// Copy data to the vector.
    pub fn copy<T: Copy>(&mut self, data: &[T]) {
        assert!(data.len() <= self.capacity());
        self.as_mut_slice::<T>()[0..data.len()].copy_from_slice(data);
    }

    pub fn unowned_ptr(&self) -> duckdb_vector {
        self.ptr
    }
}

/// A trait for inserting data into a vector.
pub trait Inserter<T> {
    /// Insert a value into the vector.
    fn insert(&self, index: usize, value: T);
}

impl Inserter<CString> for FlatVector {
    fn insert(&self, index: usize, value: CString) {
        unsafe {
            duckdb_vector_assign_string_element(self.ptr, index as u64, value.as_ptr());
        }
    }
}

impl Inserter<&str> for FlatVector {
    fn insert(&self, index: usize, value: &str) {
        let cstr = CString::new(value.as_bytes()).unwrap();
        unsafe {
            duckdb_vector_assign_string_element(self.ptr, index as u64, cstr.as_ptr());
        }
    }
}

impl Inserter<&[u8]> for FlatVector {
    fn insert(&self, index: usize, value: &[u8]) {
        let value_size = value.len();
        unsafe {
            // This function also works for binary data. https://duckdb.org/docs/api/c/api#duckdb_vector_assign_string_element_len
            duckdb_vector_assign_string_element_len(
                self.ptr,
                index as u64,
                value.as_ptr() as *const ::std::os::raw::c_char,
                value_size as u64,
            );
        }
    }
}

/// A list vector.
pub struct ListVector {
    /// ListVector does not own the vector pointer.
    entries: FlatVector,
}

impl From<duckdb_vector> for ListVector {
    fn from(ptr: duckdb_vector) -> Self {
        Self {
            entries: FlatVector::from(ptr),
        }
    }
}

impl ListVector {
    /// Returns the number of entries in the list vector.
    pub fn len(&self) -> usize {
        unsafe { duckdb_list_vector_get_size(self.entries.ptr) as usize }
    }

    /// Returns true if the list vector is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the child vector.
    // TODO: not ideal interface. Where should we keep capacity.
    pub fn child(&self, capacity: usize) -> FlatVector {
        self.reserve(capacity);
        FlatVector::with_capacity(unsafe { duckdb_list_vector_get_child(self.entries.ptr) }, capacity)
    }

    /// Take the child as [StructVector].
    pub fn struct_child(&self, capacity: usize) -> StructVector {
        self.reserve(capacity);
        StructVector::from(unsafe { duckdb_list_vector_get_child(self.entries.ptr) })
    }

    /// Take the child as [ArrayVector].
    pub fn array_child(&self) -> ArrayVector {
        ArrayVector::from(unsafe { duckdb_list_vector_get_child(self.entries.ptr) })
    }

    /// Take the child as [ListVector].
    pub fn list_child(&self) -> ListVector {
        ListVector::from(unsafe { duckdb_list_vector_get_child(self.entries.ptr) })
    }

    /// Set primitive data to the child node.
    pub fn set_child<T: Copy>(&self, data: &[T]) {
        self.child(data.len()).copy(data);
        self.set_len(data.len());
    }

    /// Set offset and length to the entry.
    pub fn set_entry(&mut self, idx: usize, offset: usize, length: usize) {
        self.entries.as_mut_slice::<duckdb_list_entry>()[idx].offset = offset as u64;
        self.entries.as_mut_slice::<duckdb_list_entry>()[idx].length = length as u64;
    }

    /// Set row as null
    pub fn set_null(&mut self, row: usize) {
        unsafe {
            duckdb_vector_ensure_validity_writable(self.entries.ptr);
            let idx = duckdb_vector_get_validity(self.entries.ptr);
            duckdb_validity_set_row_invalid(idx, row as u64);
        }
    }

    /// Reserve the capacity for its child node.
    fn reserve(&self, capacity: usize) {
        unsafe {
            duckdb_list_vector_reserve(self.entries.ptr, capacity as u64);
        }
    }

    /// Set the length of the list vector.
    pub fn set_len(&self, new_len: usize) {
        unsafe {
            duckdb_list_vector_set_size(self.entries.ptr, new_len as u64);
        }
    }
}

/// A array vector. (fixed-size list)
pub struct ArrayVector {
    ptr: duckdb_vector,
}

impl From<duckdb_vector> for ArrayVector {
    fn from(ptr: duckdb_vector) -> Self {
        Self { ptr }
    }
}

impl ArrayVector {
    /// Get the logical type of this ArrayVector.
    pub fn logical_type(&self) -> LogicalTypeHandle {
        unsafe { LogicalTypeHandle::new(duckdb_vector_get_column_type(self.ptr)) }
    }

    /// Returns the size of the array type.
    pub fn get_array_size(&self) -> u64 {
        let ty = self.logical_type();
        unsafe { duckdb_array_type_array_size(ty.ptr) as u64 }
    }

    /// Returns the child vector.
    /// capacity should be a multiple of the array size.
    // TODO: not ideal interface. Where should we keep count.
    pub fn child(&self, capacity: usize) -> FlatVector {
        FlatVector::with_capacity(unsafe { duckdb_array_vector_get_child(self.ptr) }, capacity)
    }

    /// Set primitive data to the child node.
    pub fn set_child<T: Copy>(&self, data: &[T]) {
        self.child(data.len()).copy(data);
    }

    /// Set row as null
    pub fn set_null(&mut self, row: usize) {
        unsafe {
            duckdb_vector_ensure_validity_writable(self.ptr);
            let idx = duckdb_vector_get_validity(self.ptr);
            duckdb_validity_set_row_invalid(idx, row as u64);
        }
    }
}

/// A struct vector.
pub struct StructVector {
    ptr: duckdb_vector,
}

impl From<duckdb_vector> for StructVector {
    fn from(ptr: duckdb_vector) -> Self {
        Self { ptr }
    }
}

impl StructVector {
    /// Returns the child by idx in the list vector.
    pub fn child(&self, idx: usize, capacity: usize) -> FlatVector {
        FlatVector::with_capacity(
            unsafe { duckdb_struct_vector_get_child(self.ptr, idx as u64) },
            capacity,
        )
    }

    /// Take the child as [StructVector].
    pub fn struct_vector_child(&self, idx: usize) -> StructVector {
        Self::from(unsafe { duckdb_struct_vector_get_child(self.ptr, idx as u64) })
    }

    /// Take the child as [ListVector].
    pub fn list_vector_child(&self, idx: usize) -> ListVector {
        ListVector::from(unsafe { duckdb_struct_vector_get_child(self.ptr, idx as u64) })
    }

    /// Take the child as [ArrayVector].
    pub fn array_vector_child(&self, idx: usize) -> ArrayVector {
        ArrayVector::from(unsafe { duckdb_struct_vector_get_child(self.ptr, idx as u64) })
    }

    /// Get the logical type of this struct vector.
    pub fn logical_type(&self) -> LogicalTypeHandle {
        unsafe { LogicalTypeHandle::new(duckdb_vector_get_column_type(self.ptr)) }
    }

    /// Get the name of the child by idx.
    pub fn child_name(&self, idx: usize) -> DuckDbString {
        let logical_type = self.logical_type();
        unsafe {
            let child_name_ptr = duckdb_struct_type_child_name(logical_type.ptr, idx as u64);
            DuckDbString::from_ptr(child_name_ptr)
        }
    }

    /// Get the number of children.
    pub fn num_children(&self) -> usize {
        let logical_type = self.logical_type();
        unsafe { duckdb_struct_type_child_count(logical_type.ptr) as usize }
    }

    /// Set row as null
    pub fn set_null(&mut self, row: usize) {
        unsafe {
            duckdb_vector_ensure_validity_writable(self.ptr);
            let idx = duckdb_vector_get_validity(self.ptr);
            duckdb_validity_set_row_invalid(idx, row as u64);
        }
    }
}

pub struct DictionaryVector {
    ptr: duckdb_vector,
}

impl From<duckdb_vector> for DictionaryVector {
    fn from(ptr: duckdb_vector) -> Self {
        Self { ptr }
    }
}

impl DictionaryVector {
    pub fn logical_type(&self) -> LogicalTypeHandle {
        unsafe { LogicalTypeHandle::new(duckdb_vector_get_column_type(self.ptr)) }
    }
}
