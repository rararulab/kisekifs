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

use std::{
    fmt::{Display, Formatter},
    ops::{Add, AddAssign},
};

use byteorder::{LittleEndian, WriteBytesExt};
use serde::{Deserialize, Serialize};

pub const ZERO_INO: Ino = Ino(0);
pub const ROOT_INO: Ino = Ino(1);

pub const MIN_INTERNAL_INODE: Ino = Ino(0x7FFFFFFF00000000);
pub const LOG_INODE: Ino = Ino(0x7FFFFFFF00000001);
pub const CONTROL_INODE: Ino = Ino(0x7FFFFFFF00000002);
pub const STATS_INODE: Ino = Ino(0x7FFFFFFF00000003);
pub const CONFIG_INODE: Ino = Ino(0x7FFFFFFF00000004);
pub const MAX_INTERNAL_INODE: Ino = Ino(0x7FFFFFFF10000000);

#[allow(dead_code)]
const INO_SIZE: usize = std::mem::size_of::<Ino>();

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct Ino(pub u64);

impl Display for Ino {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
}

impl AddAssign for Ino {
    fn add_assign(&mut self, rhs: Self) { self.0 += rhs.0; }
}

impl Add for Ino {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output { Self(self.0 + rhs.0) }
}

impl From<u64> for Ino {
    fn from(value: u64) -> Self { Self(value) }
}
impl From<Ino> for u64 {
    fn from(val: Ino) -> Self { val.0 }
}

impl Ino {
    pub fn is_special(&self) -> bool { *self >= MIN_INTERNAL_INODE }

    pub fn is_normal(&self) -> bool { !self.is_special() }

    pub const fn is_zero(&self) -> bool { self.0 == 0 }

    pub const fn is_root(&self) -> bool { self.0 == ROOT_INO.0 }

    // FIXME: use a better way
    // key: AiiiiiiiiI
    // key-len: 10
    pub fn generate_key(&self) -> Vec<u8> {
        let mut buf = vec![0u8; 10];
        buf.write_u8(b'A').unwrap();
        buf.write_u64::<LittleEndian>(self.0).unwrap();
        buf.write_u8(b'I').unwrap();
        buf
    }

    pub fn generate_key_str(&self) -> String { format!("A{:x}I", self.0) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ino() {
        let key = ROOT_INO.generate_key();
        println!("{key:?}");
        let key_str = ROOT_INO.generate_key_str();
        println!("{key_str:?}");
    }
}
