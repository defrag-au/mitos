//! `mitos doctor`. Every check is a named variant with a typed outcome.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::Result;
use mitos_config::{MitosFile, Profile, env};
use serde::Serialize;

use crate::cli::DoctorArgs;
use crate::init::rooted;

const GLIBC_WANT: (u32, u32) = (2, 35);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CheckName {
    Profile,
    Arch,
    Glibc,
    CgroupV2,
    Disk,
    Ram,
    Clock,
    Port,
    Health,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Outcome {
    Pass { found: String },
    Fail { wanted: String, found: String },
    Skip { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Check {
    pub name: CheckName,
    pub outcome: Outcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DoctorReport {
    pub checks: Vec<Check>,
}

impl DoctorReport {
    pub fn ok(&self) -> bool {
        !self
            .checks
            .iter()
            .any(|c| matches!(c.outcome, Outcome::Fail { .. }))
    }

    pub fn render_text(&self) -> String {
        let mut lines = Vec::new();
        for c in &self.checks {
            let name = match c.name {
                CheckName::Profile => "profile",
                CheckName::Arch => "arch",
                CheckName::Glibc => "glibc",
                CheckName::CgroupV2 => "cgroup-v2",
                CheckName::Disk => "disk",
                CheckName::Ram => "ram",
                CheckName::Clock => "clock",
                CheckName::Port => "port",
                CheckName::Health => "health",
            };
            let line = match &c.outcome {
                Outcome::Pass { found } => format!("pass  {name}: {found}"),
                Outcome::Fail { wanted, found } => {
                    format!("FAIL  {name}: wanted {wanted}, found {found}")
                }
                Outcome::Skip { reason } => format!("skip  {name}: {reason}"),
            };
            lines.push(line);
        }
        lines.join("\n")
    }
}

pub trait Host {
    fn arch(&self) -> String;
    fn glibc(&self) -> Option<(u32, u32)>;
    fn cgroup_v2(&self) -> bool;
    fn clock_unix(&self) -> i64;
    fn port_in_use(&self, addr: SocketAddr) -> bool;
    fn http_status(&self, addr: SocketAddr, path: &str) -> Option<u16>;
}

pub struct RealHost;

impl Host for RealHost {
    fn arch(&self) -> String {
        std::env::consts::ARCH.to_owned()
    }

    fn glibc(&self) -> Option<(u32, u32)> {
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        {
            // Safety: gnu_get_libc_version returns a stable NUL-terminated string.
            let raw = unsafe { std::ffi::CStr::from_ptr(libc::gnu_get_libc_version()) };
            let s = raw.to_str().ok()?;
            parse_glibc(s)
        }
        #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
        {
            None
        }
    }

    fn cgroup_v2(&self) -> bool {
        Path::new("/sys/fs/cgroup/cgroup.controllers").exists()
    }

    fn clock_unix(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    fn port_in_use(&self, addr: SocketAddr) -> bool {
        std::net::TcpListener::bind(addr).is_err()
    }

    fn http_status(&self, _addr: SocketAddr, _path: &str) -> Option<u16> {
        None
    }
}

#[cfg_attr(not(all(target_os = "linux", target_env = "gnu")), allow(dead_code))]
fn parse_glibc(s: &str) -> Option<(u32, u32)> {
    let mut nums = s
        .split(|c: char| !c.is_ascii_digit())
        .filter(|p| !p.is_empty());
    let major = nums.next()?.parse().ok()?;
    let minor = nums.next()?.parse().ok()?;
    Some((major, minor))
}

pub fn run(args: DoctorArgs) -> Result<()> {
    let report = diagnose(&args, &RealHost)?;
    if args.json {
        serde_json::to_writer_pretty(std::io::stdout(), &report)?;
        println!();
    } else {
        println!("{}", report.render_text());
    }
    if args.wait {
        anyhow::bail!("doctor --wait is not implemented until the node reports tip lag");
    }
    if !report.ok() {
        anyhow::bail!("doctor found a failing check");
    }
    Ok(())
}

pub fn diagnose(args: &DoctorArgs, host: &dyn Host) -> Result<DoctorReport> {
    let root = args.root.as_deref();
    let config_path = args
        .config_file
        .clone()
        .unwrap_or_else(|| PathBuf::from(env::DEFAULT_MITOS_TOML));
    let config_real = rooted(root, &config_path);
    let (profile, listen) = match std::fs::read_to_string(&config_real) {
        Ok(text) => {
            let file = MitosFile::parse(&text)?;
            let listen = file
                .listen_or_parse()?
                .unwrap_or_else(|| mitos_config::spec(file.network).default_listen);
            (file.profile, listen)
        }
        Err(_) => (Profile::Full, "127.0.0.1:8181".parse().unwrap()),
    };

    let health = match host.http_status(listen, "/health") {
        Some(200) => Check {
            name: CheckName::Health,
            outcome: Outcome::Pass {
                found: "GET /health 200".into(),
            },
        },
        Some(code) => Check {
            name: CheckName::Health,
            outcome: Outcome::Fail {
                wanted: "GET /health 200".into(),
                found: format!("GET /health {code}"),
            },
        },
        None => Check {
            name: CheckName::Health,
            outcome: Outcome::Skip {
                reason: "node not reachable (preflight-only)".into(),
            },
        },
    };
    let checks = vec![
        profile_check(profile),
        arch_check(host.arch()),
        glibc_check(host.glibc()),
        cgroup_check(host.cgroup_v2()),
        Check {
            name: CheckName::Disk,
            outcome: Outcome::Skip {
                reason: "full profile disk minimum is not published yet (FOOTPRINT §7)".into(),
            },
        },
        Check {
            name: CheckName::Ram,
            outcome: Outcome::Skip {
                reason: "full profile RAM minimum is not published yet (FOOTPRINT §7)".into(),
            },
        },
        clock_check(host.clock_unix()),
        port_check(listen, host.port_in_use(listen)),
        health,
    ];

    Ok(DoctorReport { checks })
}

fn profile_check(profile: Profile) -> Check {
    match profile {
        Profile::Full => Check {
            name: CheckName::Profile,
            outcome: Outcome::Pass {
                found: "full".into(),
            },
        },
        other => Check {
            name: CheckName::Profile,
            outcome: Outcome::Fail {
                wanted: "full".into(),
                found: format!("{other} (not implemented yet)"),
            },
        },
    }
}

fn arch_check(arch: String) -> Check {
    match arch.as_str() {
        "x86_64" | "aarch64" => Check {
            name: CheckName::Arch,
            outcome: Outcome::Pass { found: arch },
        },
        other => Check {
            name: CheckName::Arch,
            outcome: Outcome::Fail {
                wanted: "x86_64 or aarch64".into(),
                found: other.into(),
            },
        },
    }
}

fn glibc_check(found: Option<(u32, u32)>) -> Check {
    match found {
        None => Check {
            name: CheckName::Glibc,
            outcome: Outcome::Skip {
                reason: "not a gnu linux host".into(),
            },
        },
        Some((maj, min)) if (maj, min) >= GLIBC_WANT => Check {
            name: CheckName::Glibc,
            outcome: Outcome::Pass {
                found: format!("{maj}.{min}"),
            },
        },
        Some((maj, min)) => Check {
            name: CheckName::Glibc,
            outcome: Outcome::Fail {
                wanted: format!("{}.{}", GLIBC_WANT.0, GLIBC_WANT.1),
                found: format!("{maj}.{min}"),
            },
        },
    }
}

fn cgroup_check(v2: bool) -> Check {
    if v2 {
        Check {
            name: CheckName::CgroupV2,
            outcome: Outcome::Pass {
                found: "cgroup.controllers present".into(),
            },
        }
    } else {
        Check {
            name: CheckName::CgroupV2,
            outcome: Outcome::Fail {
                wanted: "cgroup v2 (/sys/fs/cgroup/cgroup.controllers)".into(),
                found: "absent".into(),
            },
        }
    }
}

fn clock_check(unix: i64) -> Check {
    // 2024-01-01. A box stuck in 1970 will fail Mithril and TLS.
    const WANT: i64 = 1_704_067_200;
    if unix >= WANT {
        Check {
            name: CheckName::Clock,
            outcome: Outcome::Pass {
                found: format!("unix {unix}"),
            },
        }
    } else {
        Check {
            name: CheckName::Clock,
            outcome: Outcome::Fail {
                wanted: format!("unix >= {WANT}"),
                found: format!("unix {unix}"),
            },
        }
    }
}

fn port_check(addr: SocketAddr, in_use: bool) -> Check {
    if in_use {
        Check {
            name: CheckName::Port,
            outcome: Outcome::Fail {
                wanted: format!("{addr} free"),
                found: format!("{addr} in use"),
            },
        }
    } else {
        Check {
            name: CheckName::Port,
            outcome: Outcome::Pass {
                found: format!("{addr} free"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::DoctorArgs;

    struct Fake {
        arch: String,
        glibc: Option<(u32, u32)>,
        cgroup: bool,
        clock: i64,
        port_busy: bool,
        health: Option<u16>,
    }

    impl Host for Fake {
        fn arch(&self) -> String {
            self.arch.clone()
        }
        fn glibc(&self) -> Option<(u32, u32)> {
            self.glibc
        }
        fn cgroup_v2(&self) -> bool {
            self.cgroup
        }
        fn clock_unix(&self) -> i64 {
            self.clock
        }
        fn port_in_use(&self, _: SocketAddr) -> bool {
            self.port_busy
        }
        fn http_status(&self, _: SocketAddr, _: &str) -> Option<u16> {
            self.health
        }
    }

    fn healthy() -> Fake {
        Fake {
            arch: "aarch64".into(),
            glibc: Some((2, 41)),
            cgroup: true,
            clock: 1_800_000_000,
            port_busy: false,
            health: None,
        }
    }

    fn args() -> DoctorArgs {
        DoctorArgs {
            wait: false,
            json: false,
            config_file: None,
            root: None,
        }
    }

    fn named(report: &DoctorReport, name: CheckName) -> &Outcome {
        &report
            .checks
            .iter()
            .find(|c| c.name == name)
            .unwrap()
            .outcome
    }

    #[test]
    fn preflight_passes_a_full_gnu_host() {
        let report = diagnose(&args(), &healthy()).unwrap();
        assert!(report.ok());
        assert!(matches!(
            named(&report, CheckName::Arch),
            Outcome::Pass { .. }
        ));
        assert!(matches!(
            named(&report, CheckName::Glibc),
            Outcome::Pass { .. }
        ));
        assert!(matches!(
            named(&report, CheckName::Ram),
            Outcome::Skip { .. }
        ));
    }

    #[test]
    fn each_preflight_failure_is_named() {
        let mut fake = healthy();
        fake.arch = "riscv64".into();
        fake.glibc = Some((2, 31));
        fake.cgroup = false;
        fake.clock = 0;
        fake.port_busy = true;
        let report = diagnose(&args(), &fake).unwrap();
        assert!(!report.ok());
        match named(&report, CheckName::Arch) {
            Outcome::Fail { wanted, found } => {
                assert!(wanted.contains("x86_64"));
                assert_eq!(found, "riscv64");
            }
            other => panic!("{other:?}"),
        }
        match named(&report, CheckName::Glibc) {
            Outcome::Fail { wanted, found } => {
                assert_eq!(wanted, "2.35");
                assert_eq!(found, "2.31");
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            named(&report, CheckName::CgroupV2),
            Outcome::Fail { .. }
        ));
        assert!(matches!(
            named(&report, CheckName::Clock),
            Outcome::Fail { .. }
        ));
        assert!(matches!(
            named(&report, CheckName::Port),
            Outcome::Fail { .. }
        ));
    }

    #[test]
    fn parse_glibc_reads_major_minor() {
        assert_eq!(parse_glibc("2.41"), Some((2, 41)));
        assert_eq!(parse_glibc("2.35-0ubuntu3"), Some((2, 35)));
        assert_eq!(parse_glibc("nope"), None);
    }

    #[test]
    fn json_is_typed_not_a_macro() {
        let report = diagnose(&args(), &healthy()).unwrap();
        let v = serde_json::to_value(&report).unwrap();
        assert!(v.get("checks").is_some());
        assert!(
            v.get("checks").unwrap().as_array().unwrap()[0]
                .get("name")
                .is_some()
        );
    }
}
