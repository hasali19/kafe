use std::collections::HashSet;
use std::path::Path;
use std::pin::Pin;

use eyre::{OptionExt, bail, eyre};
use prost_reflect::{FileDescriptor, MessageDescriptor};
use protobuf_native::MessageLite;
use protobuf_native::compiler::{
    SimpleErrorCollector, SourceTreeDescriptorDatabase, VirtualSourceTree,
};
use tracing::error;

use crate::schema_registry::{SchemaRegistryClient, SchemaVersionOrLatest};

pub struct DescriptorPool<'a> {
    schema_registry: &'a SchemaRegistryClient,
    descriptors: prost_reflect::DescriptorPool,
    subjects: HashSet<String>,
}

impl DescriptorPool<'_> {
    pub fn new(schema_registry: &SchemaRegistryClient) -> DescriptorPool<'_> {
        DescriptorPool {
            schema_registry,
            descriptors: Default::default(),
            subjects: HashSet::new(),
        }
    }

    /// Loads schema files for the requested subject into the descriptor poll.
    ///
    /// This will fetch the schema from schema registry, recursively fetch all referenced schemas,
    /// and compile the schemas into a descriptor set.
    pub fn register_subject(&mut self, subject: &str) -> eyre::Result<()> {
        if self.subjects.contains(subject) {
            return Ok(());
        }

        let mut source_tree = VirtualSourceTree::new();

        source_tree.as_mut().map_well_known_types();

        SchemaRegistrySourceCollector::new(self.schema_registry, source_tree.as_mut())
            .collect(subject.to_owned(), SchemaVersionOrLatest::Latest)?;

        let mut error_collector = SimpleErrorCollector::new();

        let descriptor_set = {
            let mut descriptor_db = SourceTreeDescriptorDatabase::new(source_tree.as_mut());

            descriptor_db
                .as_mut()
                .record_errors_to(error_collector.as_mut());

            descriptor_db
                .as_mut()
                .build_file_descriptor_set(&[Path::new(subject)])
        };

        let descriptor_set = match descriptor_set {
            Ok(descriptor_set) => descriptor_set,
            Err(_) => {
                for error in error_collector.as_mut() {
                    error!("{error:?}");
                }
                bail!("Failed to compile protobuf schemas")
            }
        };

        let descriptor_set_bytes = descriptor_set.serialize()?;

        self.descriptors
            .decode_file_descriptor_set(descriptor_set_bytes.as_slice())?;

        self.subjects.insert(subject.to_owned());

        Ok(())
    }

    pub fn get_file_descriptor(&mut self, subject: &str) -> eyre::Result<FileDescriptor> {
        self.register_subject(subject)?;

        self.descriptors
            .get_file_by_name(subject)
            .ok_or_eyre("Subject not found in descriptor pool")
    }

    pub fn get_message_descriptor_by_name(
        &mut self,
        subject: &str,
        message_name: Option<&str>,
    ) -> eyre::Result<MessageDescriptor> {
        let file_descriptor = self.get_file_descriptor(subject)?;
        if let Some(message_name) = message_name {
            file_descriptor
                .messages()
                .find(|message| message.name() == message_name)
                .ok_or_else(|| eyre!("Message named '{message_name}' was not found in file"))
        } else if file_descriptor.messages().len() <= 1 {
            file_descriptor
                .messages()
                .next()
                .ok_or_eyre("No messages found in schema")
        } else {
            bail!(
                "Schema contains more than one top-level message. The message name to use must be specified."
            )
        }
    }
}

struct SchemaRegistrySourceCollector<'a> {
    schema_registry: &'a SchemaRegistryClient,
    source_tree: Pin<&'a mut VirtualSourceTree>,
}

impl<'a> SchemaRegistrySourceCollector<'a> {
    fn new(
        schema_registry: &'a SchemaRegistryClient,
        source_tree: Pin<&'a mut VirtualSourceTree>,
    ) -> Self {
        Self {
            schema_registry,
            source_tree,
        }
    }

    /// Queries schema registry for the requested schema and adds it to the source tree, along with
    /// all schemas that it references.
    fn collect(&mut self, subject: String, version: SchemaVersionOrLatest) -> eyre::Result<()> {
        let schema = self
            .schema_registry
            .get_subject_schema_version(subject, version)?;

        self.source_tree
            .as_mut()
            .add_file(Path::new(&schema.subject), schema.schema.into_bytes());

        for reference in schema.references {
            self.collect(
                reference.subject,
                SchemaVersionOrLatest::Version(reference.version),
            )?;
        }

        Ok(())
    }
}
