use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "zvucast", version)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Controller {
        #[arg(long, default_value = "0.0.0.0:8080")]
        listen: String,
        #[arg(long, default_value = "/etc/zvucast/state.json")]
        state: std::path::PathBuf,
    },
    Agent {
        #[arg(long, default_value = "/etc/zvucast.toml")]
        config: std::path::PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    match Cli::parse().cmd {
        Cmd::Controller { listen, state } => {
            zvucast::controller::run(
                &listen,
                &state,
                std::path::Path::new(zvucast::controller::CONFIG_PATH),
            )
            .await?
        }
        Cmd::Agent { config } => {
            let (cfg, node_id) = zvucast::config::load_or_init_id(&config)?;
            zvucast::agent::run(cfg, node_id, env!("CARGO_PKG_VERSION").to_string()).await?;
        }
    }
    Ok(())
}
