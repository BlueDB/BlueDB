# BlueDB

### BlueDB is an on-disk, special-purpose datastore optimized for time-based data retrieval using small amounts of memory.

[Developer Quickstart Guide](https://www.bluedb.org/quick-start.html)

[BlueDB JavaDoc](https://bluedb.github.io/BlueDB)

BlueDB is simple, light on memory usage, and most importantly: __fast__ at reading a large number of objects for a specified time period.  It is written in pure Java and intended to be embedded within a Java application.  It does not support any kind of query language or API from another process.

It's written in pure Java and is single-user.  Reads are eventually consistent.  Writes are fully serializable.

BlueDB utilizes [FST](https://github.com/RuedigerMoeller/fast-serialization "FST Home") for ultra-fast object serialization/deserializaion and special file-system directory structures to build near constant-time access to records by ID or time. 

For more information visit: [bluedb.org](http://bluedb.org)

### How to execute tests and verify code coverage
- While 100% coverage isn't always worth maintaining and can cause bad habits in development, we feel that it is worth continuing to maintain this coverage at this time since it allows uncovered code to stick out like a sore thumb.  
- Start by running a coverage report on `BlueDBOnDisk/src/test/java` with `CollectionVersion#getDefault` returning `VERSION_2`. Almost everything should be covered. Look at what isn't covered and consider covering it with unit tests if possible. Then go ahead and do a coverage report for `BlueDBIntegrationE2ETests/src/test/java`. The integration tests won't have a huge coverage by themselves, but if you merge the two coverage reports then `BlueDB/src/main/java` and `BlueDBOnDisk/src/main/java` should now show that all lines covered. Note that we'd like to improve the unit testability of the `BlueDBOnDisk` codebase and migrate integration tests to the `BlueDBIntegrationE2ETests` over time.
- Run all JUnit tests in `BlueDBOnDisk/src/test/java` and `BlueDBIntegrationE2ETests/src/test/java` with `CollectionVersion#getDefault` returning `VERSION_1` and verify they all pass. There is no need to keep 100% coverage when running the test suite on the legacy version 1 collections though.  

### How to build a jar from source
Execute `gradlew onDiskJar` and a jar will be built and placed into `bluedb/build/libs`.