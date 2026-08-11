use std::{
    fs::OpenOptions,
    io,
    sync::Arc,
    time::{Duration, Instant},
};

use aws_config::{BehaviorVersion, Region, timeout::TimeoutConfig};
use batchtop::{
    app::{App, Effect, Message, execute_effect},
    aws::{BatchApi, RealAws, SharedAws},
    cli::Cli,
    terminal::{TerminalGuard, install_panic_hook},
    ui,
};
use clap::Parser;
use color_eyre::eyre::{Result, WrapErr, eyre};
use crossterm::event::{self, Event, KeyEventKind};
use tokio::sync::mpsc;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    install_panic_hook();
    let cli = Cli::parse();
    init_tracing(&cli)?;

    let timeout = TimeoutConfig::builder()
        .operation_attempt_timeout(Duration::from_secs(5))
        .operation_timeout(Duration::from_secs(15))
        .build();
    let mut loader = aws_config::defaults(BehaviorVersion::latest()).timeout_config(timeout);
    if let Some(profile) = &cli.profile {
        loader = loader.profile_name(profile);
    }
    if let Some(region) = &cli.region {
        loader = loader.region(Region::new(region.clone()));
    }
    let shared_config = loader.load().await;
    let region = shared_config
        .region()
        .map(|region| region.as_ref().to_owned())
        .ok_or_else(|| {
            eyre!("AWS Region is not configured; pass --region or configure the AWS Region provider chain")
        })?;
    info!(profile = %cli.profile_label(), %region, "starting batchtop");

    let real = Arc::new(RealAws::new(shared_config));
    let queues = real
        .discover_queues()
        .await
        .wrap_err("unable to discover AWS Batch Job Queues")?;
    info!(queues = queues.len(), "container Job Queues discovered");
    let aws: SharedAws = real;

    let mut app = App::new(cli.profile_label(), region, queues);
    let mut terminal = TerminalGuard::enter().wrap_err("unable to initialize terminal")?;
    let (sender, mut receiver) = mpsc::unbounded_channel::<Message>();
    spawn_effects(app.initial_effects(), aws.clone(), sender.clone());

    let mut render_tick = tokio::time::interval(Duration::from_millis(100));
    render_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);

    loop {
        terminal
            .terminal_mut()
            .draw(|frame| ui::render(frame, &app))
            .wrap_err("terminal render failed")?;
        if app.should_quit {
            break;
        }

        tokio::select! {
            _ = render_tick.tick() => {
                while event::poll(Duration::ZERO).wrap_err("terminal input poll failed")? {
                    if let Event::Key(key) = event::read().wrap_err("terminal input read failed")?
                        && matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat)
                    {
                        let effects = app.handle_key(key);
                        spawn_effects(effects, aws.clone(), sender.clone());
                    }
                }
                let effects = app.on_tick(Instant::now());
                spawn_effects(effects, aws.clone(), sender.clone());
            }
            Some(message) = receiver.recv() => {
                let effects = app.apply_message(message);
                spawn_effects(effects, aws.clone(), sender.clone());
            }
            result = &mut interrupt => {
                if let Err(error) = result {
                    warn!(%error, "Ctrl+C handler failed");
                }
                break;
            }
        }
    }

    Ok(())
}

fn spawn_effects(effects: Vec<Effect>, aws: SharedAws, sender: mpsc::UnboundedSender<Message>) {
    for effect in effects {
        let aws = aws.clone();
        let sender = sender.clone();
        tokio::spawn(execute_effect(aws, sender, effect));
    }
}

fn init_tracing(cli: &Cli) -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    if let Some(path) = &cli.debug_log {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .wrap_err_with(|| format!("unable to open debug log {}", path.display()))?;
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .with_writer(Arc::new(file))
            .try_init()
            .map_err(|error| eyre!(error.to_string()))?;
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(io::sink)
            .try_init()
            .map_err(|error| eyre!(error.to_string()))?;
    }
    Ok(())
}
