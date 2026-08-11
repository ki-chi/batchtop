use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Clone, Parser, PartialEq, Eq)]
#[command(
    name = "batchtop",
    version,
    about = "Read-only AWS Batch monitoring TUI"
)]
pub struct Cli {
    /// AWS shared-configuration profile name.
    #[arg(short, long)]
    pub profile: Option<String>,

    /// AWS Batch Region. Overrides the standard AWS Region provider chain.
    #[arg(short, long)]
    pub region: Option<String>,

    /// Write diagnostic logs to this file instead of the active terminal.
    #[arg(long, value_name = "PATH")]
    pub debug_log: Option<PathBuf>,
}

impl Cli {
    pub fn profile_label(&self) -> String {
        self.profile
            .clone()
            .or_else(|| std::env::var("AWS_PROFILE").ok())
            .unwrap_or_else(|| "<provider>".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Cli;

    #[test]
    fn parses_profile_region_and_debug_log() {
        let cli = Cli::try_parse_from([
            "batchtop",
            "-p",
            "research-prod",
            "-r",
            "ap-northeast-1",
            "--debug-log",
            "batchtop.log",
        ])
        .unwrap();

        assert_eq!(cli.profile.as_deref(), Some("research-prod"));
        assert_eq!(cli.region.as_deref(), Some("ap-northeast-1"));
        assert_eq!(
            cli.debug_log.as_deref(),
            Some(std::path::Path::new("batchtop.log"))
        );
    }
}
