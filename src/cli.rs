use clap::builder::styling::AnsiColor;

#[derive(clap::Parser)]
#[command(styles = styles())]
pub struct Args {
    /// One or more kafka broker addresses
    ///
    /// Alternatively set the KAFE_KAFKA_BROKERS environment variable.
    #[clap(long)]
    pub brokers: Option<String>,

    /// Schema registry address
    ///
    /// Alternatively set the KAFE_SCHEMA_REGISTRY environment variable.
    #[clap(long)]
    pub schema_registry: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(clap::Subcommand)]
pub enum Command {
    /// Produce records to a topic
    ///
    /// Records are read line by line from stdin, in json format.
    Produce {
        /// Name of the topic
        topic: String,

        /// Name of the protobuf message to use
        ///
        /// If the schema resolved for the specified topic contains only a single message, it is
        /// used by default. This option can be used to specify the name of the message to use, and
        /// is required if the schema contains more than one message definition.
        message: Option<String>,
    },

    /// Consume records from a topic
    ///
    /// Records are written to stdout, in json format.
    Consume {
        /// Name of the topic
        topic: String,
    },

    /// Tombstone all keys on a topic
    Tombstone { topic: String },
}

fn styles() -> clap::builder::Styles {
    clap::builder::Styles::styled()
        .header(AnsiColor::Yellow.on_default())
        .usage(AnsiColor::Green.on_default())
        .literal(AnsiColor::Green.on_default())
        .placeholder(AnsiColor::Green.on_default())
}
