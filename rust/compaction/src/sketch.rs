//! This module provides some extra required wrappers around sketches
//! functionality such as common traits.
/*
 * Copyright 2022-2024 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use crate::aws_s3::ObjectStoreFactory;
use arrow::array::{ArrayAccessor, AsArray};
use arrow::datatypes::{
    BinaryType, DataType, Int32Type, Int64Type, LargeBinaryType, LargeUtf8Type, Schema, Utf8Type,
};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use bytes::{BufMut, Bytes};
use cxx::{Exception, UniquePtr};
use log::info;
use num_format::{Locale, ToFormattedString};
use rust_sketch::quantiles::byte::{byte_sketch_t, new_byte_sketch};
use rust_sketch::quantiles::i32::{i32_sketch_t, new_i32_sketch};
use rust_sketch::quantiles::i64::{i64_sketch_t, new_i64_sketch};
use rust_sketch::quantiles::str::{new_str_sketch, string_sketch_t};
use std::io::Write;
use std::iter::zip;
use std::mem::size_of;
use std::sync::Arc;
use url::Url;

/// Constant size for quantiles data sketches
const K: u16 = 1024;

pub enum DataSketchVariant {
    I32(UniquePtr<i32_sketch_t>),
    I64(UniquePtr<i64_sketch_t>),
    Str(DataType, UniquePtr<string_sketch_t>),
    Bytes(DataType, UniquePtr<byte_sketch_t>),
}

pub trait Item {
    fn to_i32(&self) -> Option<i32>;
    fn to_i64(&self) -> Option<i64>;
    fn to_str(&self) -> Option<&str>;
    fn to_bytes(&self) -> Option<&[u8]>;
}

impl Item for i32 {
    fn to_i32(&self) -> Option<i32> {
        Some(*self)
    }

    fn to_i64(&self) -> Option<i64> {
        None
    }

    fn to_str(&self) -> Option<&str> {
        None
    }

    fn to_bytes(&self) -> Option<&[u8]> {
        None
    }
}

impl Item for i64 {
    fn to_i32(&self) -> Option<i32> {
        None
    }

    fn to_i64(&self) -> Option<i64> {
        Some(*self)
    }

    fn to_str(&self) -> Option<&str> {
        None
    }

    fn to_bytes(&self) -> Option<&[u8]> {
        None
    }
}

impl Item for String {
    fn to_i32(&self) -> Option<i32> {
        None
    }

    fn to_i64(&self) -> Option<i64> {
        None
    }

    fn to_str(&self) -> Option<&str> {
        Some(self.as_ref())
    }

    fn to_bytes(&self) -> Option<&[u8]> {
        None
    }
}

impl Item for &str {
    fn to_i32(&self) -> Option<i32> {
        None
    }

    fn to_i64(&self) -> Option<i64> {
        None
    }

    fn to_str(&self) -> Option<&str> {
        Some(self)
    }

    fn to_bytes(&self) -> Option<&[u8]> {
        None
    }
}

impl Item for Vec<u8> {
    fn to_i32(&self) -> Option<i32> {
        None
    }

    fn to_i64(&self) -> Option<i64> {
        None
    }

    fn to_str(&self) -> Option<&str> {
        None
    }

    fn to_bytes(&self) -> Option<&[u8]> {
        Some(self.as_ref())
    }
}

impl Item for &[u8] {
    fn to_i32(&self) -> Option<i32> {
        None
    }

    fn to_i64(&self) -> Option<i64> {
        None
    }

    fn to_str(&self) -> Option<&str> {
        None
    }

    fn to_bytes(&self) -> Option<&[u8]> {
        Some(self)
    }
}

#[allow(dead_code)]
impl DataSketchVariant {
    /// Updates this sketch variant with the given value.
    ///
    /// Calls the update method of the underlying sketch.
    ///
    /// # Panics
    /// If the value provided cannot be converted to the type of this [`DataSketch`], i.e.
    /// if you try to update an i32 sketch with a string.
    #[allow(clippy::needless_pass_by_value)]
    pub fn update<T>(&mut self, value: T)
    where
        T: Item,
    {
        match self {
            DataSketchVariant::I32(s) => s.pin_mut().update(value.to_i32().unwrap()),
            DataSketchVariant::I64(s) => s.pin_mut().update(value.to_i64().unwrap()),
            DataSketchVariant::Str(_, s) => s.pin_mut().update(value.to_str().unwrap()),
            DataSketchVariant::Bytes(_, s) => s.pin_mut().update(value.to_bytes().unwrap()),
        }
    }

    /// Get the minimum item from this sketch.
    ///
    /// # Errors
    /// If the sketch is empty an error is thrown.
    pub fn get_min_item(&self) -> Result<Box<dyn Item>, Exception> {
        match self {
            DataSketchVariant::I32(s) => Ok(s.get_min_item().map(Box::new)?),
            DataSketchVariant::I64(s) => Ok(s.get_min_item().map(Box::new)?),
            DataSketchVariant::Str(_, s) => Ok(s.get_min_item().map(Box::new)?),
            DataSketchVariant::Bytes(_, s) => Ok(s.get_min_item().map(Box::new)?),
        }
    }

    /// Get the maximum item from this sketch.
    ///
    /// # Errors
    /// If the sketch is empty an error is thrown.
    pub fn get_max_item(&self) -> Result<Box<dyn Item>, Exception> {
        match self {
            DataSketchVariant::I32(s) => Ok(s.get_max_item().map(Box::new)?),
            DataSketchVariant::I64(s) => Ok(s.get_max_item().map(Box::new)?),
            DataSketchVariant::Str(_, s) => Ok(s.get_max_item().map(Box::new)?),
            DataSketchVariant::Bytes(_, s) => Ok(s.get_max_item().map(Box::new)?),
        }
    }

    /// Serialise this data sketch to bytes.
    ///
    /// This serialisation is compatible with the Java equivalent.
    ///
    /// # Errors
    /// If the sketch couldn't be serialised at the C++ layer.
    ///
    pub fn serialize(&self, header_size_bytes: u32) -> Result<Vec<u8>, Exception> {
        match self {
            DataSketchVariant::I32(s) => s.serialize(header_size_bytes),
            DataSketchVariant::I64(s) => s.serialize(header_size_bytes),
            DataSketchVariant::Str(_, s) => s.serialize(header_size_bytes),
            DataSketchVariant::Bytes(_, s) => s.serialize(header_size_bytes),
        }
    }

    /// Return the underlying Arrow [`DataType`] this sketch is intended for.
    #[must_use]
    pub fn data_type(&self) -> DataType {
        match self {
            DataSketchVariant::I32(_) => DataType::Int32,
            DataSketchVariant::I64(_) => DataType::Int64,
            DataSketchVariant::Str(t, _) | DataSketchVariant::Bytes(t, _) => t.clone(),
        }
    }

    /// Create a new sketch for the given Arrow [`DataType`].
    ///
    /// The `k` parameter is passed to the underlying datasketch. See
    /// Apache Data Sketch documentation for an explanation.
    ///
    /// # Panics
    /// If the given [`DataType`] is not supported.
    #[must_use]
    pub fn new(d: &DataType, k: u16) -> DataSketchVariant {
        match d {
            DataType::Int32 => DataSketchVariant::I32(new_i32_sketch(k)),
            DataType::Int64 => DataSketchVariant::I64(new_i64_sketch(k)),
            t @ (DataType::Utf8 | DataType::LargeUtf8) => {
                DataSketchVariant::Str(t.clone(), new_str_sketch(k))
            }
            t @ (DataType::Binary | DataType::LargeBinary) => {
                DataSketchVariant::Bytes(t.clone(), new_byte_sketch(k))
            }
            _ => {
                panic!("DataType not supported {d}");
            }
        }
    }
}

/// Write the data sketches to the named file.
///
/// For each sketch, the length of the serialised sketch is written in
/// big endian format, compatible with [`DataOutputStream::writeInt`](https://docs.oracle.com/en/java/javase/20/docs/api/java.base/java/io/DataOutputStream.html#writeInt(int))
/// followed by the bytes of the sketch.
///
/// # Errors
/// Any I/O errors are wrapped into an [`ArrowError`] if thrown.
/// The data sketch serialisation might also throw errors from the underlying
/// data sketch library.
#[allow(clippy::cast_possible_truncation)]
pub fn serialise_sketches(
    store_factory: &ObjectStoreFactory,
    path: &Url,
    sketches: &[DataSketchVariant],
) -> Result<(), ArrowError> {
    let mut buf = vec![].writer();

    let mut size = 0;
    // for each sketch write the size i32, followed by bytes
    for sketch in sketches {
        let serialised = sketch
            .serialize(0)
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
        buf.write_all(&(serialised.len() as u32).to_be_bytes())?;
        buf.write_all(&serialised)?;
        size += serialised.len() + size_of::<u32>();
    }

    //Path part of S3 URL
    let store_path = object_store::path::Path::from(path.path());

    // Save to object store
    let store = store_factory.get_object_store(path)?;

    futures::executor::block_on(store.put(&store_path, Bytes::from(buf.into_inner())))
        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

    info!(
        "Serialised {} ({} bytes) sketches to {}",
        sketches.len(),
        size.to_formatted_string(&Locale::en),
        path
    );
    Ok(())
}

/// Update the data sketches for the column numbers in row key fields list.
///
/// Works through each value in each column indexed by `row_key_fields` and
/// updates the corresponding data sketch i the array of sketches.
///
/// # Panics
/// Panic if length of sketch array is different to number of row key fields.
/// Panic if `row_key_fields` contains column number that isn't in schema.
/// Panic if there is a mismatch between the sketch data type and the column type.
pub fn update_sketches(
    b: &RecordBatch,
    sketches: &mut [DataSketchVariant],
    row_key_fields: impl AsRef<[usize]>,
) {
    assert!(
        sketches.len() == row_key_fields.as_ref().len(),
        "Differing number of sketches and row key fields!"
    );
    // Take each sketch and row key column in turn, then update the sketch
    for (sketch, column) in zip(
        sketches,
        row_key_fields.as_ref().iter().map(|&index| b.column(index)),
    ) {
        // dynamic dispatch. Match the datatype to the type of sketch to update.
        match column.data_type() {
            DataType::Int32 => update_sketch(sketch, &column.as_primitive::<Int32Type>()),
            DataType::Int64 => update_sketch(sketch, &column.as_primitive::<Int64Type>()),
            DataType::Utf8 => update_sketch(
                sketch,
                &column.as_string::<<Utf8Type as arrow::datatypes::ByteArrayType>::Offset>(),
            ),
            DataType::LargeUtf8 => update_sketch(
                sketch,
                &column.as_string::<<LargeUtf8Type as arrow::datatypes::ByteArrayType>::Offset>(),
            ),
            DataType::Binary => update_sketch(
                sketch,
                &column.as_binary::<<BinaryType as arrow::datatypes::ByteArrayType>::Offset>(),
            ),
            DataType::LargeBinary => update_sketch(
                sketch,
                &column.as_binary::<<LargeBinaryType as arrow::datatypes::ByteArrayType>::Offset>(),
            ),
            _ => panic!("Row type {} not supported", column.data_type()),
        }
    }
}

/// Update the given sketch from an array.
///
/// The list of values in the given array are updated into the data sketch.
///
/// # Panics
/// Panic if sketch type is not compatible with the item type of the array.
fn update_sketch<T: crate::sketch::Item, A: ArrayAccessor<Item = T>>(
    sketch: &mut DataSketchVariant,
    array: &A,
) {
    for i in 0..array.len() {
        unsafe {
            sketch.update(array.value_unchecked(i));
        }
    }
}

/// Create a vector of Data Sketches.
///
/// This creates the appropriate data sketch implementations based on on the row key fields
/// and the data types in the schema. Each type is wrapped in a [`SketchEnum`] variant type.
///
/// # Panics
/// If a row key field can't be found in the schema.
///
pub fn make_sketches_for_schema(
    schema: &Arc<Schema>,
    row_key_fields: &impl AsRef<[usize]>,
) -> Vec<DataSketchVariant> {
    row_key_fields
        .as_ref()
        .iter()
        .map(|&index| DataSketchVariant::new(schema.field(index).data_type(), K))
        .collect()
}
