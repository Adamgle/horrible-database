# Horrible Database

> What started as a joke, remained one.

**Horrible Database** is a file-system-based database written in Rust. It was done for a Rust HTTP server dummy application that I have created for testing.

## How it works

It's a document-based database, that exposes an API to create collections whose type has to share a trait implementation. Buffers the operations on the database
in a Write-ahead log fashion and executes them every constant `WAL_COMMAND_SIZE`. The idea was to postpone the deserialization of the collections to allow
O(1) operations that mutate the state of the database. This approach also saves memory, since collections are not kept fully in-memory — important when collections grow large. The system uses `type_tag` to tag entries buffered in the WAL, making it possible to deserialize them into trait objects later.

## Features

- File-based persistence using JSON serialization
- Write-Ahead Log (WAL) for durability — postpones deserializing collection files until they are actually needed
- Asynchronous and multithreaded operations powered by Tokio

## Why is it "Horrible"?

- Performance degrades as the database grows (roughly quadratic complexity for WAL flushes)
- Data has to be fully brought in-memory when flushing the Write-ahead log (frees afterwards).
- Not suitable for real-world use—meant for learning and experimentation
- When collections grow big, it would not be possible to serve data in a reasonable timing.
- For operations like selects, it has to flush the buffered commands in the WAL, which would require deserializing the collection making selects absolute disaster in time complexity.

## Usage

Don't use it.

## Disclaimer

This project is for educational purposes only. Would not recommend it to anyone. For real-world applications, use a proper database that do not offer O(n) worst-case time complexity on selects.

## License

MIT
