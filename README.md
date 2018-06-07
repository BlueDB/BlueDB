# BlueDB

### BlueDB is a pure Java datastore designed to retreive Java objects by time and/or ID in near constant time. 

BlueDB is simple, lightweight, and most importantly: __fast__. It does not support any kind of query language or API from another process. It is intended to be embedded within a Java application. BlueDB utilizes [FST](https://github.com/RuedigerMoeller/fast-serialization "FST Home") for ultra-fast object serialization/deserializaion and special file-system directory structures to build near constant-time access to records by ID or time. 
