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

use pyroscope::{PyroscopeAgent, pyroscope::PyroscopeAgentRunning};
use pyroscope_pprofrs::{PprofConfig, pprof_backend};
use snafu::{ResultExt, Whatever};

use crate::env::var;

pub type Guard = PyroscopeAgent<PyroscopeAgentRunning>;

pub fn init_pyroscope() -> Result<Option<Guard>, Whatever> {
    let url = var("PYROSCOPE_SERVER_URL")?;
    if url.is_none() {
        return Ok(None);
    }

    let url = url.unwrap();
    println!("found pyroscope url: {url}, starting pyroscope agent ...");

    let agent = PyroscopeAgent::builder(url, String::from("kiseki"))
        .backend(pprof_backend(PprofConfig::new().sample_rate(100)))
        .build()
        .with_whatever_context(|_e| "failed to setup pyroscope agent")?;

    let agent_running = agent
        .start()
        .with_whatever_context(|_e| "failed to start pyroscope agent")?;

    Ok(Some(agent_running))
}
