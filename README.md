# Horrible Database

> What started as a joke has remained one.

**Horrible Database** is a file-system-based database written in Rust. It was build on the side and isolated to separate project for a Rust HTTP server dummy application that I built for testing.

## How it works

It's a document-based database that exposes an API to create collections whose type has to share a trait implementation. It buffers the operations on the database
in a Write-Ahead Log fashion and executes them every constant `WAL_COMMAND_SIZE`.

The idea was to postpone the deserialization of the collections to allow
O(1) operations that mutate the state of the database. This approach also saves memory, since collections are not kept fully in-memory — which is important when collections grow large. The system uses `type_tag` to tag entries buffered in the WAL, making it possible to deserialize them into trait objects later.

## Features

- File-based persistence using JSON serialization
- Write-Ahead Log (WAL) for durability — postpones deserializing collection files until they are actually needed
- Asynchronous and multithreaded operations powered by Tokio

## Why is it "Horrible"?

- Performance degrades as the database grows (roughly quadratic complexity for WAL flushes)
- Data has to be fully brought into memory when flushing the Write-Ahead Log (freed afterwards), not indexing and pagination.
- When collections grow large, it is not possible to serve data in a reasonable time
- For operations like SELECTs, it has to flush the buffered commands in the WAL, which requires deserializing the collection, making SELECTs an absolute disaster in time complexity

## How could we improve it?

- Flushing the Write-Ahead Log to collection should not be a blocking operation, it is now as it blocks the Mutex to DatabaseCollections for time of execution.
  We would need to rethink infrastructure to allow operation keep scheduling in the WAL file while the previous are getting written to the collections. That would pose some issues with synchronizing the state of collections.
  It would improve the performance if there are low amount of data writes, but average to the same performance when writing quickly I suppose.
- Pagination applied to the data so we could avoid parsing the whole collection.

## Usage

Not suitable for real-world use — meant for learning and experimentation

**Don't use it.**

## Disclaimer

This project is for educational purposes only. I would not recommend it to anyone. For real-world applications, use a proper database that does not offer O(n) worst-case time complexity on SELECTs.

## License

MIT
