# BlueDB

### BlueDB is an on-disk, special-purpose datastore optimized for time-based data retrieval using small amounts of memory.

[Developer Quickstart Guide](https://www.bluedb.org/quick-start.html)

[BlueDB JavaDoc](https://bluedb.github.io/BlueDB)

BlueDB is simple, light on memory usage, and most importantly: __fast__ at reading a large number of objects for a specified time period.  It is written in pure Java and intended to be embedded within a Java application.  It does not support any kind of query language or API from another process.

It's written in pure Java and is single-user.  Reads are eventually consistent.  Writes are fully serializable.

BlueDB utilizes [FST](https://github.com/RuedigerMoeller/fast-serialization "FST Home") for ultra-fast object serialization/deserializaion and special file-system directory structures to build near constant-time access to records by ID or time. 

For more information visit: [bluedb.org](http://bluedb.org)

### How to fix compile errors in the integration tests project
The `BlueDBIntegrationE2ETests` project depends on the test source sets of the `BlueDBOnDisk` project. The way we define that in gradle currently requires you to run `gradlew compileTestJava` before the `BlueDBIntegrationE2ETests` will be able to see that code.

### How to execute tests and verify code coverage
- Use your IDE to run the unit tests in `BlueDBOnDisk/src/test/java`. If you do a coverage report then `BlueDB/src/main/java` and `BlueDBOnDisk/src/main/java` should have all lines covered. While 100% coverage isn't always worth maintaining and can cause bad habbits in development, we feel that it is worth continuing to maintain this coverage at this time since it allows uncovered code to stick out like a sore thumb.  
- You should also use your IDE to run the unit tests in `BlueDBIntegrationE2ETests/src/test/java` and make sure that they all pass. You don't need to worry about coverage with these tests.  
- You should execute the tests with `CollectionVersion#getDefault` returning VERSION_1 and with it returning VERSION_2 in order to ensure that everything works for both supported collection versions.  

### How to build a jar from source
Execute `gradlew onDiskJar` and a jar will be built and placed into `bluedb/build/libs`.