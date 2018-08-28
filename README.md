# BlueDB

### BlueDB is an on-disk, special-purpose datastore optimized for time-based reporting using small amounts of memory.

BlueDB is simple, light on memory usage, and most importantly: __fast__ at reading a large number of objects for a specified time period.  It is written in pure Java and intended to be embedded within a Java application.  It does not support any kind of query language or API from another process.

It's written in pure Java and is single-user.  Reads are eventually consistent.  Writes are fully serializable.

BlueDB utilizes [FST](https://github.com/RuedigerMoeller/fast-serialization "FST Home") for ultra-fast object serialization/deserializaion and special file-system directory structures to build near constant-time access to records by ID or time. 
