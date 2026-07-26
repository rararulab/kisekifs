// Copyright 2024 kisekifs
//
// JuiceFS, Copyright 2020 Juicedata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use tokio::sync::RwLock;

use crate::{
    backend::{BackendRef, key::Counter},
    err::Result,
};

/// A table for allocating inode numbers.
/// It starts at 2 since the root inode is 1.
pub struct IdTable {
    next_max_pair: RwLock<(u64, u64)>,
    backend:       BackendRef,
    counter:       Counter,
}

impl IdTable {
    /// Return a new empty `IdTable`.
    pub(crate) fn new(backend: BackendRef, counter: Counter) -> Self {
        Self {
            next_max_pair: RwLock::new((0, 0)),
            backend,
            counter,
        }
    }

    /// Return the next unused ID from the table.
    // The write guard is deliberately held across `increase_count_by` to
    // serialize ID allocation (the read-modify-write must be atomic), so the
    // lint's early-drop suggestion does not apply.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn next(&self) -> Result<u64> {
        let mut next_max_pair = self.next_max_pair.write().await;
        if next_max_pair.0 >= next_max_pair.1 {
            let step = self.counter.get_step();
            let new_max = self.backend.increase_count_by(self.counter, step)?;
            next_max_pair.0 = new_max - step as u64;
            next_max_pair.1 = new_max;
        }
        let mut next = next_max_pair.0;
        next_max_pair.0 += 1;
        while next <= 1 {
            next = next_max_pair.0;
            next_max_pair.0 += 1;
        }
        Ok(next)
    }
}
