// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is copied from https://github.com/tikv/raft-engine/blob/8dd2a39f359ff16f5295f35343f626e0c10132fa/src/util.rs

use std::{
    fmt::{self, Debug, Display, Write},
    ops::{Div, Mul},
    str::FromStr,
};

use serde::{
    Deserialize, Deserializer, Serialize, Serializer, de,
    de::{Unexpected, Visitor},
};

const UNIT: u64 = 1;

const BINARY_DATA_MAGNITUDE: u64 = 1024;
pub const B: u64 = UNIT;
pub const KIB: u64 = B * BINARY_DATA_MAGNITUDE;
pub const MIB: u64 = KIB * BINARY_DATA_MAGNITUDE;
pub const GIB: u64 = MIB * BINARY_DATA_MAGNITUDE;
pub const TIB: u64 = GIB * BINARY_DATA_MAGNITUDE;
pub const PIB: u64 = TIB * BINARY_DATA_MAGNITUDE;

#[derive(Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub struct ReadableSize(pub u64);

impl ReadableSize {
    pub const fn kb(count: u64) -> Self { Self(count * KIB) }

    pub const fn mb(count: u64) -> Self { Self(count * MIB) }

    pub const fn gb(count: u64) -> Self { Self(count * GIB) }

    pub const fn as_mb(self) -> u64 { self.0 / MIB }

    pub const fn as_bytes(self) -> u64 { self.0 }

    pub const fn as_bytes_usize(self) -> usize { self.0 as usize }
}

impl Div<u64> for ReadableSize {
    type Output = Self;

    fn div(self, rhs: u64) -> Self { Self(self.0 / rhs) }
}

impl Div<Self> for ReadableSize {
    type Output = u64;

    fn div(self, rhs: Self) -> u64 { self.0 / rhs.0 }
}

impl Mul<u64> for ReadableSize {
    type Output = Self;

    fn mul(self, rhs: u64) -> Self { Self(self.0 * rhs) }
}

impl Serialize for ReadableSize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let size = self.0;
        let mut buffer = String::new();
        if size == 0 {
            write!(buffer, "{size}KiB").unwrap();
        } else if size.is_multiple_of(PIB) {
            write!(buffer, "{}PiB", size / PIB).unwrap();
        } else if size.is_multiple_of(TIB) {
            write!(buffer, "{}TiB", size / TIB).unwrap();
        } else if size.is_multiple_of(GIB) {
            write!(buffer, "{}GiB", size / GIB).unwrap();
        } else if size.is_multiple_of(MIB) {
            write!(buffer, "{}MiB", size / MIB).unwrap();
        } else if size.is_multiple_of(KIB) {
            write!(buffer, "{}KiB", size / KIB).unwrap();
        } else {
            return serializer.serialize_u64(size);
        }
        serializer.serialize_str(&buffer)
    }
}

impl FromStr for ReadableSize {
    type Err = String;

    // This method parses value in binary unit.
    fn from_str(s: &str) -> Result<Self, String> {
        let size_str = s.trim();
        if size_str.is_empty() {
            return Err(format!("{s:?} is not a valid size."));
        }

        if !size_str.is_ascii() {
            return Err(format!("ASCII string is expected, but got {s:?}"));
        }

        // size: digits and '.' as decimal separator
        let size_len = size_str
            .to_string()
            .chars()
            .take_while(|c| char::is_ascii_digit(c) || ['.', 'e', 'E', '-', '+'].contains(c))
            .count();

        // unit: alphabetic characters
        let (size, unit) = size_str.split_at(size_len);

        let unit = match unit.trim() {
            "K" | "KB" | "KiB" => KIB,
            "M" | "MB" | "MiB" => MIB,
            "G" | "GB" | "GiB" => GIB,
            "T" | "TB" | "TiB" => TIB,
            "P" | "PB" | "PiB" => PIB,
            "B" | "" => B,
            _ => {
                return Err(format!(
                    "only B, KB, KiB, MB, MiB, GB, GiB, TB, TiB, PB, and PiB are supported: {s:?}"
                ));
            }
        };

        match size.parse::<f64>() {
            Ok(n) => Ok(Self((n * unit as f64) as u64)),
            Err(_) => Err(format!("invalid size string: {s:?}")),
        }
    }
}

impl Debug for ReadableSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{self}") }
}

impl Display for ReadableSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 >= PIB {
            write!(f, "{:.1}PiB", self.0 as f64 / PIB as f64)
        } else if self.0 >= TIB {
            write!(f, "{:.1}TiB", self.0 as f64 / TIB as f64)
        } else if self.0 >= GIB {
            write!(f, "{:.1}GiB", self.0 as f64 / GIB as f64)
        } else if self.0 >= MIB {
            write!(f, "{:.1}MiB", self.0 as f64 / MIB as f64)
        } else if self.0 >= KIB {
            write!(f, "{:.1}KiB", self.0 as f64 / KIB as f64)
        } else {
            write!(f, "{}B", self.0)
        }
    }
}

impl<'de> Deserialize<'de> for ReadableSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SizeVisitor;

        impl<'de> Visitor<'de> for SizeVisitor {
            type Value = ReadableSize;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("valid size")
            }

            fn visit_i64<E>(self, size: i64) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                if size >= 0 {
                    self.visit_u64(size as u64)
                } else {
                    Err(E::invalid_value(Unexpected::Signed(size), &self))
                }
            }

            fn visit_u64<E>(self, size: u64) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                Ok(ReadableSize(size))
            }

            fn visit_str<E>(self, size_str: &str) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                size_str.parse().map_err(E::custom)
            }
        }

        deserializer.deserialize_any(SizeVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_readable_size() {
        let s = ReadableSize::kb(2);
        assert_eq!(s.0, 2048);
        assert_eq!(s.as_mb(), 0);
        let s = ReadableSize::mb(2);
        assert_eq!(s.0, 2 * 1024 * 1024);
        assert_eq!(s.as_mb(), 2);
        let s = ReadableSize::gb(2);
        assert_eq!(s.0, 2 * 1024 * 1024 * 1024);
        assert_eq!(s.as_mb(), 2048);

        assert_eq!((ReadableSize::mb(2) / 2).0, MIB);
        assert_eq!((ReadableSize::mb(1) / 2).0, 512 * KIB);
        assert_eq!(ReadableSize::mb(2) / ReadableSize::kb(1), 2048);
    }
}
