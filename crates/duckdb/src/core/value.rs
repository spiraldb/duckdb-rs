use crate::ffi::{duckdb_destroy_value, duckdb_get_int64, duckdb_get_varchar, duckdb_value};
use libduckdb_sys::{
    duckdb_create_blob, duckdb_create_bool, duckdb_create_date, duckdb_create_double, duckdb_create_float,
    duckdb_create_int16, duckdb_create_int32, duckdb_create_int64, duckdb_create_int8, duckdb_create_null_value,
    duckdb_create_time, duckdb_create_timestamp, duckdb_create_timestamp_ms, duckdb_create_timestamp_ns,
    duckdb_create_timestamp_s, duckdb_create_uint16, duckdb_create_uint32, duckdb_create_uint64, duckdb_create_uint8,
    duckdb_date, duckdb_time, duckdb_timestamp, duckdb_timestamp_ms, duckdb_timestamp_ns, duckdb_timestamp_s,
    duckdb_timestamp_struct,
};
use std::{ffi::CString, fmt};

/// The Value object holds a single arbitrary value of any type that can be
/// stored in the database.
#[derive(Debug)]
pub struct Value {
    pub(crate) ptr: duckdb_value,
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value>,
{
    fn from(t: Option<T>) -> Self {
        match t {
            Some(t) => t.into(),
            None => Value::null(),
        }
    }
}

impl Value {
    pub fn null() -> Value {
        Value {
            ptr: unsafe { duckdb_create_null_value() },
        }
    }

    pub fn date_from_day_count(value: i32) -> Value {
        Self {
            ptr: unsafe { duckdb_create_date(duckdb_date { days: value }) },
        }
    }

    pub fn time_from_ms(value: i64) -> Value {
        Self {
            ptr: unsafe { duckdb_create_time(duckdb_time { micros: value }) },
        }
    }

    pub fn timestamp_s(seconds: i64) -> Value {
        Self {
            ptr: unsafe { duckdb_create_timestamp_s(duckdb_timestamp_s { seconds }) },
        }
    }

    pub fn timestamp_ms(millis: i64) -> Value {
        Self {
            ptr: unsafe { duckdb_create_timestamp_ms(duckdb_timestamp_ms { millis }) },
        }
    }

    pub fn timestamp_us(micros: i64) -> Value {
        Self {
            ptr: unsafe { duckdb_create_timestamp(duckdb_timestamp { micros }) },
        }
    }

    pub fn timestamp_ns(nanos: i64) -> Value {
        Self {
            ptr: unsafe { duckdb_create_timestamp_ns(duckdb_timestamp_ns { nanos }) },
        }
    }
}

impl From<duckdb_value> for Value {
    fn from(ptr: duckdb_value) -> Self {
        Self { ptr }
    }
}

impl From<&str> for Value {
    fn from(ptr: &str) -> Self {
        ptr.as_bytes().into()
    }
}

impl From<&[u8]> for Value {
    fn from(ptr: &[u8]) -> Self {
        unsafe {
            Self {
                ptr: duckdb_create_blob(ptr.as_ptr(), ptr.len() as u64),
            }
        }
    }
}

impl Drop for Value {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                duckdb_destroy_value(&mut self.ptr);
            }
        }
        self.ptr = std::ptr::null_mut();
    }
}

impl Value {
    /// Returns the value as a int64
    pub fn to_int64(&self) -> i64 {
        unsafe { duckdb_get_int64(self.ptr) }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let c_string = unsafe { CString::from_raw(duckdb_get_varchar(self.ptr)) };
        write!(f, "{}", c_string.to_str().expect("cannot extract c_str"))
    }
}

macro_rules! impl_duckdb_create_value {
    ($ty:ty, $ddb_fn:ident) => {
        impl From<$ty> for Value {
            fn from(value: $ty) -> Self {
                Value {
                    ptr: unsafe { $ddb_fn(value) },
                }
            }
        }
    };
}

impl_duckdb_create_value!(bool, duckdb_create_bool);
impl_duckdb_create_value!(i8, duckdb_create_int8);
impl_duckdb_create_value!(i16, duckdb_create_int16);
impl_duckdb_create_value!(i32, duckdb_create_int32);
impl_duckdb_create_value!(i64, duckdb_create_int64);
impl_duckdb_create_value!(u8, duckdb_create_uint8);
impl_duckdb_create_value!(u16, duckdb_create_uint16);
impl_duckdb_create_value!(u32, duckdb_create_uint32);
impl_duckdb_create_value!(u64, duckdb_create_uint64);
impl_duckdb_create_value!(f32, duckdb_create_float);
impl_duckdb_create_value!(f64, duckdb_create_double);
