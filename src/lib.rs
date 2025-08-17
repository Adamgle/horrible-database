/*
    ### What started as a joke, remained one ###
    NOTE: That was does purely for educational purposes, I would not recommend using this in production as it's a file system based database and reinvented wheel (rectangular one).
*/
#![allow(non_snake_case)]

// SIDE NOTE: The main issue of why the performance of this web server is soo bad, is because of this database,
// which does have to read => deserialize => write to deserialize collection => write serialized every WAL_COMMAND_SIZE constant,
// making it roughly run in quadratic complexity of WAL file flush.

// ### NOTES ###
// This database is deeply flawed, as insertions are not done in O(1) append only time, since we have to parse the file to
// add a new entry. Implementing theE WAL file, which is append only, we could postpone that process, but that is not even the
// biggest issue that is resolved, we still have to bring the data to memory, parse it and operate on it to insert new entries. That also
// means that if we would want to select an entry we would have to flush the WAL file to the database file to provide stateful output.
// It is easy to see that when database grows, the performance of the database would degrade.
// #############

// The problem with database is that it has it's tradeoffs with the possible implementations of it that I am seeing;
// The main issue is that it is just a file which posses a lot of tradeoffs:
// 1. To add a new entry, we have to read the whole file, parse it, add the new entry, serialize it and write it back
// to optimize the process when have 2 solutions that I see:
// 1.1 We could hold a buffer for parsed database at some time, keep it sync with what is in the file which itself posses a risk
// and hold another field with a link to a file opened for writing so that we could avoid the sys call to open a file, thought probably
// we would have to put it into the Arc<Mutex<T>>
// 1.2 We could do that in one take when writing an entry, or just storing the reference to file, thought that is not that much of a performance improvement
// All of the above we have been avoided if we would just use SQL.

// The other way of doing that could be to keep something like a temp file that stores the `commands` that should be execute on the database
// thought that file should be session based, otherwise the file could also grow big, assuming we would execute those commands on server
// clean up, actually the better way would be to flush it more often then.
// This seems like a better approach because, we would not have to parse the whole database file every so often, thought for actual
// modifications we would have to parse the whole file, thought that would be done less often then the other way around.
// Maybe for the commands file to now grow to big we could execute those when like 100 commands are in the file,

// We'll build a command system, for now we only need insertion, thought something like Update, Delete, Select would be nice
// Easiest way thought not most performant is JSON file with commands.
// QUESTION: When do we execute the commands
// ANSWER: We are creating a file instead of storing it in memory because we want to persist the commands in case    of server crash.
// For performance reasons we could store it in memory, but given that the tasks are async, working independently of each other
// that would involve Arc<Mutex<T>>, also executing the commands on certain threshold or on system shutdown must be done on separate thread
// so to avoid blocking the IO overhead, though the IO is async from tokio, thought don't sure if windows supports async IO

pub mod collections;
pub mod prelude;

pub use crate::collections::DatabaseEntryTrait;
use crate::collections::{
    CollectionNamesFiles, DatabaseCollections, DatabaseConfigEntry, HttpRequestError,
};

// use crate::config::config_file::DatabaseConfigEntry;
// use crate::http::HttpRequestError;

use crate::prelude::*;

use std::collections::HashSet;
use std::error::Error;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use strum::VariantNames;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
};

#[derive(Debug)]
pub struct Database {
    pub collections: DatabaseCollections,
}

impl Database {
    pub async fn new(config: &DatabaseConfigEntry) -> Result<Self, Box<dyn Error + Send + Sync>> {
        return Ok(Self {
            collections: DatabaseCollections::new(config.clone()).await?,
        });
    }
}

/// Stores the write-ahead log file for each `DatabaseEntry`, is `DatabaseCollection` agnostic.
/// Currently initialization is done in the `Config` constructor, and stored there for the duration of the program.
#[derive(Debug)]
struct DatabaseWAL {
    /// File handle to the underlying write-ahead log file.
    handler: Arc<Mutex<File>>,
    /// Count of the commands that are in the WAL file, used to determine when to flush the WAL file to the database file.
    size: Arc<AtomicUsize>,
    /// Used to keep track of the `DatabaseEntry`s is the WAL file for parsing to avoid later iteration.
    collection_names: HashSet<String>,
}

/// The size of the WAL file, when it reaches that size, it will be flushed to the database file.
/// Greater in size the better performance, may result with higher memory usage, but I think it's negligible.
///
/// There is also possibility to opt-out of slushing on certain size, do it only on commands
/// like select, select_all to provide stateful output, but otherwise just flush it on shutdown.
/// To opt-out set it to max value of `usize`.

impl DatabaseWAL {
    pub const WAL_COMMAND_SIZE: usize = 100;

    async fn new(
        config: &DatabaseConfigEntry,
    ) -> Result<Arc<Mutex<Self>>, Box<dyn Error + Send + Sync>> {
        Ok(Arc::new(Mutex::new(Self {
            handler: Self::open_file(&config).await?,
            size: Arc::new(AtomicUsize::new(0)),
            collection_names: HashSet::new(),
        })))
    }

    /// Loads the state of the WAL file and parses it to instance of `DatabaseWAL`.
    /// Could happen when server closes abruptly and we need to restore the state of the `DatabaseWAL` instance.
    async fn load_state(
        config: &DatabaseConfigEntry,
    ) -> Result<Option<Arc<Mutex<Self>>>, Box<dyn Error + Send + Sync>> {
        if let Ok(result) = fs::try_exists(&config.WAL).await {
            if result {
                let metadata = fs::metadata(&config.WAL).await?;

                if metadata.len() != 0 {
                    let instance = DatabaseWAL::new(config).await?;
                    let mut WAL = instance.lock().await;

                    let commands = DatabaseWAL::parse_WAL(WAL.get_handler(), WAL.get_size())
                        .await
                        .inspect_err(|e| error!("Failed to parse WAL file: {e}"))?;

                    WAL.collection_names
                        .extend(commands.iter().map(|command| command.get_collection_name()));

                    drop(WAL);

                    return Ok(Some(Arc::clone(&instance)));
                }
            }
        };

        // We want a pattern of Result<Option<T>> for it to not trigger and error if there is no unprocessed commands in the WAL file.
        // as it is not technically an error.
        Ok(None)
    }

    /// Deserializes the WAL file to the `Vec` of raw commands, just as they were written. Takes file handler to WAL file and `size` of WAL file defined on instance.
    ///
    /// `NOTE`: Is defined as a static method to work both in `execute_commands` and `DatabaseWAL::load_state`, otherwise deadlock would occur
    /// if we would acquire lock from the instance, we could also do it as the method and then pass the MutexGuard, but I think it's better.
    ///
    /// `size` parameter would be 0 if used from `load_state`, but that would just not pre-allocate the capacity of the `Vec`.
    async fn parse_WAL(
        handler: Arc<Mutex<File>>,
        size: usize,
    ) -> Result<Vec<DatabaseCommand>, Box<dyn Error + Send + Sync>> {
        let mut handler = handler.lock().await;

        let mut buffer = Vec::<DatabaseCommand>::with_capacity(size);

        handler.seek(std::io::SeekFrom::Start(0)).await?;

        let mut reader = BufReader::new(&mut *handler).lines();

        while let Some(line) = reader.next_line().await.inspect_err(|e| {
            error!("Potentially corrupted DatabaseCommand in WAL file with: {e}.")
        })? {
            let command = DatabaseCommand::deserialize(&line)
                .inspect_err(|_| error!("Failed to deserialize DatabaseCommand from WAL file."))?;
            buffer.push(command);
        }

        Ok(buffer)
    }

    /// Opens file handle to the underling write-ahead log or creates if not exists.
    async fn open_file(
        config: &DatabaseConfigEntry,
    ) -> Result<Arc<Mutex<File>>, Box<dyn Error + Send + Sync>> {
        // Create the logging file for the commands
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(&config.WAL)
            .await?;

        Ok(Arc::new(Mutex::new(file)))
    }

    /// executes the command on the WAL file,
    async fn save_command(
        &mut self,
        command: DatabaseCommand,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Below we will write the actual entry to the WAL file.

        let file = self.get_handler();
        let mut file = file.lock().await;

        let mut command_json = serde_json::to_string(&command)?;
        command_json.push_str("\r\n");

        let command_json = command_json.as_bytes();

        file.write_all(command_json).await?;
        file.flush().await?;

        self.add_collection_name(command.get_collection_name())
            .inspect_err(|e| {
                error!("Failed to add collection name to the set of known collection names: {e}");
            })?;

        self.increment_size();

        Ok(())
    }

    fn get_handler(&self) -> Arc<Mutex<File>> {
        Arc::clone(&self.handler)
    }

    fn get_size(&self) -> usize {
        self.size.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn increment_size(&self) {
        self.size.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    /// Resets the size of `size` field to 0 and the size of the WAL file to 0.
    async fn reset_size(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let file = self.get_handler();
        let mut file = file.lock().await;

        file.set_len(0)
            .await
            .inspect_err(|_| error!("Failed to reset the size of WAL file."))?;

        file.seek(std::io::SeekFrom::Start(0))
            .await
            .inspect_err(|_| error!("Failed to reset the size of WAL file."))?;

        self.size.store(0, std::sync::atomic::Ordering::SeqCst);

        Ok(())
    }

    /// Adds a collection name to the set of known collection names.
    ///
    /// The names here actually reflect the collection_name of T in the typetag::serde(name = T)
    /// and the T should exist in the enum CollectionNamesFiles.
    fn add_collection_name(
        &mut self,
        collection_name: String,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if !CollectionNamesFiles::VARIANTS.contains(&collection_name.to_lowercase().as_str()) {
            return Err(Box::<dyn Error + Send + Sync>::from(HttpRequestError {
                status_code: 500,
                status_text: "Internal Server Error".into(),
                content_type: Some("text/plain".into()),
                internals: Some(Box::<dyn Error + Send + Sync>::from(format!(
                    "Collection name given in the typetag::serde(name = T) does not exist in the enum; {collection_name} not found as the enum variant",
                ))),
                ..Default::default()
            }));
        }

        self.collection_names.insert(collection_name.to_lowercase());
        Ok(())
    }

    fn get_collection_names(&self) -> &HashSet<String> {
        &self.collection_names
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// IO buffered commands execute on the database, on system shutdown or 100 commands in the buffer (memory or WAL file I think).
pub enum DatabaseCommand {
    /// Insert one entry to given DatabaseEntry with a new entry
    Insert { entry: Box<dyn DatabaseEntryTrait> },
    Update {
        /// The entry to update, should be a new entry with the same id.
        entry: Box<dyn DatabaseEntryTrait>,
        // The entry holds it's type in the tagged fashion, as it is parsed when first inserted, deleted, whatever command is used, it would be tagged. So we know what type
        // we are dealing with.
        /// The id of the entry to update.
        id: String,
    },
    /// Delete one entry to given Box<dyn DatabaseEntryTrait>
    Delete {
        // entry: Box<dyn DatabaseEntryTrait>,
        collection_name: String,
        /// The id of the entry to delete.
        id: String,
    },
    /// Select one entry to given Box<dyn Box<dyn DatabaseEntryTrait>Trait>
    ///
    /// `Select` and `SelectAll` are commands that cannot be buffered in the WAL file,
    /// as they are not triggering the side effect on database, so they will not be stored in the WAL file
    /// if execute, but evaluated eagerly and they will also trigger the execute_commands of the buffered commands, as if not, the result could be stale.
    Select {
        /// The entry to select.
        collection_name: String,
        id: String,
    },
    /// Select everything to given Box<dyn DatabaseEntryTrait>
    SelectAll { collection_name: String },
}

impl DatabaseCommand {
    /// Returns the lowercased T as declared in the serde::typetag(name = T)
    fn get_collection_name(&self) -> String {
        use DatabaseCommand::*;
        match self {
            Insert { entry } | Update { entry, .. } => entry.typetag_name().to_string(),
            Select {
                collection_name, ..
            }
            | SelectAll { collection_name }
            | Delete {
                collection_name, ..
            } => collection_name.clone(),
        }
        .to_lowercase()
    }

    fn deserialize(entry: &impl AsRef<[u8]>) -> Result<Self, serde_json::Error> {
        serde_json::from_slice::<Self>(entry.as_ref())
            .inspect_err(|e| error!("Failed to deserialize DatabaseCommand: {e}"))
    }
}
