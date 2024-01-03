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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;

use crate::raw::{Access, AccessorInfo};
use crate::{Builder, Result, Scheme};

use super::native::{ConnectedRados, Rados};

/// Ceph service support through librados.
#[derive(Debug, Default)]
pub struct RadosBuilder {
    cluster: Option<String>,
    username: Option<String>,
    config: RadosConfig,
}

impl RadosBuilder {
    /// Set the cluster's name.
    pub fn cluster<T>(&mut self, cluster: T) -> &mut Self
    where
        T: Into<String>,
    {
        let cluster = cluster.into();
        self.cluster = if cluster.is_empty() {
            None
        } else {
            Some(cluster)
        };
        self
    }

    /// Set the user ID used for connecting to the remote cluster.
    ///
    /// The user ID should be fully qualified.
    pub fn username<T>(&mut self, username: T) -> &mut Self
    where
        T: Into<String>,
    {
        let username = username.into();
        self.username = if username.is_empty() {
            None
        } else {
            Some(username)
        };
        self
    }

    /// Set the value of the specified rados configuration key.
    ///
    /// If the key is already set, its associated value will be overwritten.
    ///
    /// # Panic
    ///
    /// This function will panic if the builder is already configured to load configurations from environment variable
    /// or configuration file.
    pub fn set_config<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        if let RadosConfig::Kv(kv) = &mut self.config {
            let key = key.into();
            let value = value.into();
            kv.insert(key, value);
            self
        } else {
            panic!("the RadosBuilder is already configured to load from config file or env");
        }
    }

    /// Configure the builder to load rados configuration from the specified configuration file when building.
    ///
    /// # Panic
    ///
    /// This function will panic if any of the following conditions are met:
    /// - One or more configuration keys has already been set via the `set_config` function;
    /// - The builder has already been configured to load configuration from configuration file;
    /// - The builder has already been configured to load configuration from environment variable.
    pub fn load_config_from_file<P>(&mut self, file_path: P) -> &mut Self
    where
        P: Into<PathBuf>,
    {
        if let RadosConfig::Kv(kv) = &self.config {
            assert!(kv.is_empty());
            self.config = RadosConfig::FromFile(file_path.into());
            self
        } else {
            panic!("the RaodsBuilder is already configured to load from config file or env");
        }
    }

    /// Configure the builder to load rados configuration from the specified environment variable when building.
    ///
    /// # Panic
    ///
    /// This function will panic if any of the following conditions are met:
    /// - One or more configuration keys has already been set via the `set_config` function;
    /// - The builder has already been configured to load configuration from configuration file;
    /// - The builder has already been configured to load configuration from environment variable.
    pub fn load_config_from_env<T>(&mut self, env_name: T) -> &mut Self
    where
        T: Into<String>,
    {
        if let RadosConfig::Kv(kv) = &self.config {
            assert!(kv.is_empty());
            self.config = RadosConfig::FromEnv(env_name.into());
            self
        } else {
            panic!("the RaodsBuilder is already configured to load from config file or env");
        }
    }
}

impl Builder for RadosBuilder {
    const SCHEME: Scheme = Scheme::Rados;
    type Accessor = RadosBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        Self {
            cluster: None,
            username: None,
            config: RadosConfig::Kv(map),
        }
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        // Create rados object.
        let mut rados = Rados::new(self.cluster.as_deref(), self.username.as_deref())
            .map_err(|err| err.to_opendal_error(crate::ErrorKind::Unexpected, "create rados"))?;

        // Set rados configuration.
        match &self.config {
            RadosConfig::Kv(kv) => {
                for (k, v) in kv {
                    rados.set_config(k.clone(), v.clone()).map_err(|err| {
                        err.to_opendal_error(crate::ErrorKind::ConfigInvalid, "invalid config")
                    })?;
                }
            }
            RadosConfig::FromFile(file_path) => {
                rados
                    .load_config_from_file(file_path.to_string_lossy().as_ref())
                    .map_err(|err| {
                        err.to_opendal_error(
                            crate::ErrorKind::ConfigInvalid,
                            "load config from file",
                        )
                    })?;
            }
            RadosConfig::FromEnv(env_name) => {
                rados
                    .load_config_from_env(env_name.clone())
                    .map_err(|err| {
                        err.to_opendal_error(
                            crate::ErrorKind::ConfigInvalid,
                            "load config from env",
                        )
                    })?;
            }
        };

        // And connect to the remote cluster.
        let connected_rados = rados.connect().map_err(|err| {
            err.to_opendal_error(crate::ErrorKind::Unexpected, "connect to cluster")
        })?;

        Ok(RadosBackend::new(connected_rados))
    }
}

/// Backend for rados services.
#[derive(Clone, Debug)]
pub struct RadosBackend {
    raw: Arc<ConnectedRados>,
}

impl RadosBackend {
    fn new(rados: ConnectedRados) -> Self {
        Self {
            raw: Arc::new(rados),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Access for RadosBackend {
    // TODO: implement the following associated types.
    type Reader;
    type Writer;
    type Lister;
    type BlockingReader;
    type BlockingWriter;
    type BlockingLister;

    fn info(&self) -> AccessorInfo {
        todo!()
    }
}

#[derive(Debug)]
enum RadosConfig {
    Kv(HashMap<String, String>),
    FromFile(PathBuf),
    FromEnv(String),
}

impl Default for RadosConfig {
    fn default() -> Self {
        Self::Kv(HashMap::new())
    }
}
