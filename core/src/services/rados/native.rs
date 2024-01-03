// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::ffi::{c_char, CStr, CString};
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll, Waker};

use ceph::rados::{rados_callback_t, rados_completion_t, rados_ioctx_t, rados_list_ctx_t, rados_t};
use chrono::{DateTime, Utc};
use libc::{c_int, c_void, time_t, ENOENT};

/// A Rust-friendly wrapper around a ceph RADOS error code.
#[derive(Debug)]
pub struct Error(pub c_int);

impl Error {
    /// Check the given return value from a native rados function. If it is a negative value,
    /// return an `Err` value indicating the error. Otherwise, return an `Ok` value containing
    /// the given value as-is.
    pub fn guard(value: c_int) -> Result<c_int, Self> {
        if value >= 0 {
            Ok(value)
        } else {
            Err(Self(-value))
        }
    }

    /// Call the given function and check its return value using the `guard` function.
    pub fn guard_call<F>(func: F) -> Result<c_int, Self>
    where
        F: FnOnce() -> c_int,
    {
        Self::guard(func())
    }

    /// Convert this error into an OpenDAL error.
    pub fn to_opendal_error<T>(&self, kind: crate::ErrorKind, op: T) -> crate::Error
    where
        T: Into<&'static str>,
    {
        crate::Error::new(kind, &format!("{}", *self)).with_operation(op)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        const MESSAGE_BUFFER_SIZE: usize = 256;

        let mut msg_buf = vec![0i8; MESSAGE_BUFFER_SIZE];
        let get_msg_res =
            unsafe { libc::strerror_r(self.0, msg_buf.as_mut_ptr(), MESSAGE_BUFFER_SIZE) };
        if get_msg_res != 0 {
            // Failed to get the error message. Display the error code directly.
            return write!(f, "error code: 0x{:x}", self.0);
        }

        let msg_cstr = unsafe { CStr::from_ptr(msg_buf.as_ptr()) };
        let msg_str = msg_cstr.to_string_lossy();
        write!(f, "{}", msg_str)
    }
}

impl std::error::Error for Error {}

/// A Rust-friendly wrapper around a `rados_t` value that is not connected to any clusters yet.
///
/// We don't choose to use `ceph::ceph::Rados` because it lacks some features that are necessary in
/// OpenDAL. Specifically:
///
///   - It does not support setting configurations before connecting to a cluster.
///   - It requires you to use a configuration file.
#[derive(Debug)]
pub struct Rados(rados_t);

impl Rados {
    /// Create a new rados instance.
    ///
    /// Internally this function creates a new `rados_t` via the `rados_create2` function.
    pub fn new(cluster: Option<&str>, username: Option<&str>) -> Result<Self, Error> {
        let native_cluster = cluster.map(|c| CString::new(c).unwrap());
        let native_username = username.map(|u| CString::new(u).unwrap());

        let native_cluster_ptr = native_cluster
            .map(|s| s.as_ptr())
            .unwrap_or(std::ptr::null());
        let native_username_ptr = native_username
            .map(|s| s.as_ptr())
            .unwrap_or(std::ptr::null());

        let mut raw_rados: rados_t = std::ptr::null_mut();
        Error::guard_call(|| unsafe {
            ceph::rados::rados_create2(&mut raw_rados, native_cluster_ptr, native_username_ptr, 0)
        })?;

        Ok(Self(raw_rados))
    }

    /// Set a configuration option.
    pub fn set_config<K, V>(&mut self, key: K, value: V) -> Result<(), Error>
    where
        K: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
    {
        let native_key = CString::new(key).unwrap();
        let native_value = CString::new(value).unwrap();
        Error::guard_call(|| unsafe {
            ceph::rados::rados_conf_set(self.0, native_key.as_ptr(), native_value.as_ptr())
        })?;
        Ok(())
    }

    /// Load configurations from the specified configuration file.
    pub fn load_config_from_file<P>(&mut self, path: P) -> Result<(), Error>
    where
        P: Into<Vec<u8>>,
    {
        let native_path = CString::new(path).unwrap();
        Error::guard_call(|| unsafe {
            ceph::rados::rados_conf_read_file(self.0, native_path.as_ptr())
        })?;
        Ok(())
    }

    /// Parse and set configurations from the specified environment variable.
    pub fn load_config_from_env<E>(&mut self, var: E) -> Result<(), Error>
    where
        E: Into<Vec<u8>>,
    {
        let native_var = CString::new(var).unwrap();
        Error::guard_call(|| unsafe {
            ceph::rados::rados_conf_parse_env(self.0, native_var.as_ptr())
        })?;
        Ok(())
    }

    /// Connect this `rados_t` value to the configured cluster and return a `ConnectedRados` that
    /// represents the connected `rados_t` value.
    pub fn connect(self) -> Result<ConnectedRados, Error> {
        Error::guard_call(|| unsafe { ceph::rados::rados_connect(self.0) })?;
        Ok(ConnectedRados(self.0))
    }
}

/// A Rust-friendly wrapper around a `rados_t` value that has been connected to a cluster.
#[derive(Debug)]
pub struct ConnectedRados(rados_t);

impl ConnectedRados {
    /// Connect to the specified pool and open an IO context associated with the pool.
    pub fn create_ioctx<N>(&self, pool_name: N) -> Result<IoCtx, Error>
    where
        N: Into<Vec<u8>>,
    {
        let native_pool_name = CString::new(pool_name).unwrap();
        let mut raw_ioctx: rados_ioctx_t = std::ptr::null_mut();
        Error::guard_call(|| unsafe {
            ceph::rados::rados_ioctx_create(self.0, native_pool_name.as_ptr(), &mut raw_ioctx)
        })?;
        Ok(IoCtx(raw_ioctx, PhantomData))
    }
}

impl Drop for ConnectedRados {
    fn drop(&mut self) {
        unsafe {
            ceph::rados::rados_shutdown(self.0);
        }
    }
}

unsafe impl Send for ConnectedRados {}
unsafe impl Sync for ConnectedRados {}

/// A Rust-friendly wrapper around a `rados_ioctx_t` value that represents an IO context.
#[derive(Debug)]
pub struct IoCtx<'a>(rados_ioctx_t, PhantomData<&'a ConnectedRados>);

impl<'a> IoCtx<'a> {
    /// Write data into the specified object. Existing data will be overwritten.
    pub fn write<N>(&self, oid: N, data: &[u8]) -> Result<(), Error>
    where
        N: Into<Vec<u8>>,
    {
        let native_oid = CString::new(oid).unwrap();
        Error::guard_call(|| unsafe {
            ceph::rados::rados_write_full(
                self.0,
                native_oid.as_ptr(),
                data.as_ptr() as *const i8,
                data.len(),
            )
        })?;
        Ok(())
    }

    /// Append data to the end of the specified object.
    pub fn append<N>(&self, oid: N, data: &[u8]) -> Result<(), Error>
    where
        N: Into<Vec<u8>>,
    {
        let native_oid = CString::new(oid).unwrap();
        Error::guard_call(|| unsafe {
            ceph::rados::rados_append(
                self.0,
                native_oid.as_ptr(),
                data.as_ptr() as *const i8,
                data.len(),
            )
        })?;
        Ok(())
    }

    /// Read data from the specified object into the given buffer.
    ///
    /// This function returns the number of bytes successfully read.
    pub fn read<N>(&self, oid: N, buffer: &mut [u8]) -> Result<usize, Error>
    where
        N: Into<Vec<u8>>,
    {
        let native_oid = CString::new(oid).unwrap();
        let bytes_read = Error::guard_call(|| unsafe {
            ceph::rados::rados_read(
                self.0,
                native_oid.as_ptr(),
                buffer.as_mut_ptr() as *mut i8,
                buffer.len(),
                0,
            )
        })?;
        Ok(bytes_read as usize)
    }

    /// Remove the specified object.
    pub fn remove<N>(&self, oid: N) -> Result<(), Error>
    where
        N: Into<Vec<u8>>,
    {
        let native_oid = CString::new(oid).unwrap();
        Error::guard_call(|| unsafe { ceph::rados::rados_remove(self.0, native_oid.as_ptr()) })?;
        Ok(())
    }

    /// Get statistical information about the specified object.
    pub fn stat<N>(&self, oid: N) -> Result<ObjectStat, Error>
    where
        N: Into<Vec<u8>>,
    {
        let native_oid = CString::new(oid).unwrap();
        let mut size: u64 = 0;
        let mut mtime: time_t = 0;
        Error::guard_call(|| unsafe {
            ceph::rados::rados_stat(self.0, native_oid.as_ptr(), &mut size, &mut mtime)
        })?;
        Ok(ObjectStat {
            size,
            modified_time: DateTime::from_timestamp(mtime, 0).unwrap(),
        })
    }

    /// Create an `ObjectIter` that iterates the objects.
    pub fn list_objects(&self) -> Result<ObjectIter<'_, 'a>, Error> {
        let mut raw_list_ctx: rados_list_ctx_t = std::ptr::null_mut();
        Error::guard_call(|| unsafe {
            ceph::rados::rados_nobjects_list_open(self.0, &mut raw_list_ctx)
        })?;
        Ok(ObjectIter(raw_list_ctx, PhantomData))
    }
}

/// Statistical information about an object.
#[derive(Clone, Debug)]
pub struct ObjectStat {
    /// The total size of the object.
    pub size: u64,

    /// The modified time of the object.
    pub modified_time: DateTime<Utc>,
}

/// List objects within a pool.
#[derive(Debug)]
pub struct ObjectIter<'i, 'a>(rados_list_ctx_t, PhantomData<&'i IoCtx<'a>>);

impl<'i, 'a> Drop for ObjectIter<'i, 'a> {
    fn drop(&mut self) {
        unsafe {
            ceph::rados::rados_nobjects_list_close(self.0);
        }
    }
}

impl<'i, 'a> Iterator for ObjectIter<'i, 'a> {
    type Item = Result<ObjectEntry, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut raw_entry: *const c_char = std::ptr::null();
        let mut raw_key: *const c_char = std::ptr::null();
        let mut raw_nspace: *const c_char = std::ptr::null();

        let ret = unsafe {
            rados_nobjects_list_next(self.0, &mut raw_entry, &mut raw_key, &mut raw_nspace)
        };

        if ret == -ENOENT {
            return None;
        } else if let Err(err) = Error::guard(ret) {
            return Some(Err(err));
        }

        Some(Ok(ObjectEntry {
            entry: unsafe { CStr::from_ptr(raw_entry) }
                .to_str()
                .unwrap()
                .to_owned(),
            key: unsafe { CStr::from_ptr(raw_key) }
                .to_str()
                .unwrap()
                .to_owned(),
            namespace: unsafe { CStr::from_ptr(raw_nspace) }
                .to_str()
                .unwrap()
                .to_owned(),
        }))
    }
}

/// An entry yielded by [`ObjectIter`].
#[derive(Clone, Debug)]
pub struct ObjectEntry {
    /// The name of the entry.
    pub entry: String,

    /// The object locator.
    pub key: String,

    /// The object namespace.
    pub namespace: String,
}

// The native RADOS binding provided by the ceph-rust crate is wrong on the `rados_nobjects_list_next` function.
// So provide our own for that.

#[cfg(unix)]
#[link(name = "rados", kind = "dylib")]
extern "C" {
    fn rados_nobjects_list_next(
        ctx: rados_list_ctx_t,
        entry: *mut *const ::libc::c_char,
        key: *mut *const ::libc::c_char,
        nspace: *mut *const ::libc::c_char,
    ) -> ::libc::c_int;
}

/// Wrap a `rado_completion_t` object, provide a Rust future for polling the completion status.
#[derive(Debug)]
struct Completion {
    raw: rados_completion_t,
    state: Arc<Mutex<CompletionState>>,
}

impl Completion {
    /// Create a new `Completion` that is ready when the underlying completion object is completed.
    fn ready_on_complete() -> Result<Self, Error> {
        Self::new(true)
    }

    /// Create a new `Completion` that is ready when the underlying completion object is safe.
    fn ready_on_safe() -> Result<Self, Error> {
        Self::new(false)
    }

    fn new(ready_on_complete: bool) -> Result<Self, Error> {
        let state = Arc::new(Mutex::new(CompletionState::default()));

        let mut cb_complete: rados_callback_t = None;
        let mut cb_safe: rados_callback_t = None;
        let target_cb = if ready_on_complete {
            &mut cb_complete
        } else {
            &mut cb_safe
        };
        *target_cb = Some(completion_callback);

        let cb_arg = Arc::into_raw(state.clone()) as *mut c_void;

        let mut raw: rados_completion_t = std::ptr::null_mut();
        Error::guard_call(|| unsafe {
            ceph::rados::rados_aio_create_completion(cb_arg, cb_complete, cb_safe, &mut raw)
        })?;

        Ok(Self { raw, state })
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        unsafe {
            ceph::rados::rados_aio_release(self.raw);
        }
    }
}

impl Future for Completion {
    type Output = c_int;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let mut state_lock = self.state.lock().unwrap();
        if let Some(ret_value) = state_lock.ret_value {
            return Poll::Ready(ret_value);
        }

        state_lock.waker = Some(cx.waker().clone());

        Poll::Pending
    }
}

extern "C" fn completion_callback(raw: rados_completion_t, arg: *mut c_void) {
    let state = unsafe { Arc::from_raw(arg as *const Mutex<CompletionState>) };

    let ret_value = unsafe { ceph::rados::rados_aio_get_return_value(raw) };

    let mut state_lock = state.lock().unwrap();
    assert!(state_lock.ret_value.is_none());

    state_lock.ret_value = Some(ret_value);

    if let Some(waker) = &mut state_lock.waker {
        waker.wake_by_ref();
    }
}

#[derive(Debug, Default)]
struct CompletionState {
    ret_value: Option<c_int>,
    waker: Option<Waker>,
}
