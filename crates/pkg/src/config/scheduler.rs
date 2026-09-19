use serde::Deserialize;

/// Config for `core_module` scheduler.
///
/// Time of the run is fully decided by this config. Stored inside `crates/pkg`
/// so both binaries and `core_module` share a single source of truth.
///
/// Loaded via `from_env()` (dotenvy + `SCHEDULER_*` env vars), consistent with
/// `CoordinatorConfig` / `WorkerConfig`.
#[derive(Debug, Clone, Deserialize)]
pub struct SchedulerConfig {
    /// Master switch. When false the scheduler does not tick at all.
    pub enabled: bool,
    /// Cron expression (5-field standard `min hour dom mon dow` or 6-field with seconds).
    /// Example: `"0 2 * * *"` = daily 02:00, `"*/5 * * * *"` = every 5 minutes.
    pub cron: String,
    /// IANA timezone name, e.g. `UTC`, `Asia/Jakarta`, `Asia/Singapore`.
    /// Defaults to `UTC`. Parsed with `chrono-tz` in the scheduler.
    pub timezone: String,
}

fn default_enabled() -> bool {
    true
}

fn default_cron() -> String {
    "0 2 * * *".into() // daily at 02:00
}

fn default_timezone() -> String {
    "UTC".into()
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            cron: default_cron(),
            timezone: default_timezone(),
        }
    }
}

impl SchedulerConfig {
    pub fn from_env() -> Self {
        let _ = dotenvy::dotenv();
        Self {
            enabled: std::env::var("SCHEDULER_ENABLED")
                .ok()
                .map(|v| parse_bool(&v))
                .unwrap_or_else(default_enabled),
            cron: std::env::var("SCHEDULER_CRON")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(default_cron),
            timezone: std::env::var("SCHEDULER_TIMEZONE")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(default_timezone),
        }
    }

    /// Quick validation without requiring the `cron` crate in `pkg`.
    /// The full `Schedule::from_str` validation happens in `core_module::scheduler`.
    pub fn is_valid(&self) -> bool {
        !self.cron.trim().is_empty() && !self.timezone.trim().is_empty()
    }
}

fn parse_bool(s: &str) -> bool {
    matches!(
        s.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on" | "y" | "t"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_values() {
        let c = SchedulerConfig::default();
        assert!(c.enabled);
        assert_eq!(c.cron, "0 2 * * *");
        assert_eq!(c.timezone, "UTC");
    }

    #[test]
    fn parse_bool_truthy() {
        assert!(parse_bool("true"));
        assert!(parse_bool("TRUE"));
        assert!(parse_bool("1"));
        assert!(parse_bool("yes"));
        assert!(parse_bool("on"));
    }

    #[test]
    fn parse_bool_falsy() {
        assert!(!parse_bool("false"));
        assert!(!parse_bool("0"));
        assert!(!parse_bool("no"));
        assert!(!parse_bool(""));
    }
}
