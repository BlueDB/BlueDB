package org.bluedb.disk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

import org.bluedb.api.BlueCollectionVersion;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.ActiveTimeKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.collection.ReadWriteTimeCollectionOnDisk;
import org.bluedb.disk.helpers.BlueDbOnDiskWrapper;
import org.bluedb.disk.helpers.BlueDbOnDiskWrapper.StartupOption;
import org.junit.Test;

public class EmptyDirectoriesAfterDeleteIntegrationTest {
	@Test
	public void test_directoryEmptyAfterBasicInsertAndDelete() throws IOException, BlueDbException, InterruptedException, ExecutionException {
		try (BlueDbOnDiskWrapper dbWrapper = new BlueDbOnDiskWrapper(StartupOption.EncryptionDisabled, BlueCollectionVersion.VERSION_2)) {
			
			ReadWriteTimeCollectionOnDisk<TestValue> timeCollection = dbWrapper.getTimeCollection();
			
			TestValue testValue = new TestValue("Bob", 3);
			TimeKey timeKey = new TimeKey(1, 1);
			
			Path dataDirectoryPath = timeCollection.getPath().resolve("0");
			Path fileChunkPath = dataDirectoryPath.resolve("0/0/0/1_1");
			
			timeCollection.insert(timeKey, testValue);
			
			assertEquals(testValue, timeCollection.get(timeKey));
			assertTrue(Files.exists(fileChunkPath));
			assertTrue(Files.exists(dataDirectoryPath));
			
			timeCollection.delete(timeKey);
			
			assertEquals(null, timeCollection.get(timeKey));
			assertFalse(Files.exists(fileChunkPath));
			
			timeCollection.getRollupScheduler().forceScheduleRollups(true); //Rollups should delete the empty directories
			
			assertFalse(Files.exists(dataDirectoryPath));
			assertTrue(Files.exists(timeCollection.getPath()));
		}
	}
	
	@Test
	public void test_fileEmptyAfterBasicInsertAndDelete() throws IOException, BlueDbException {
		try (BlueDbOnDiskWrapper dbWrapper = new BlueDbOnDiskWrapper(StartupOption.EncryptionDisabled, BlueCollectionVersion.VERSION_2)) {
			
			ReadWriteTimeCollectionOnDisk<TestValue> timeCollection = dbWrapper.getTimeCollection();
			
			TestValue testValue1 = new TestValue("Bob", 3);
			TimeKey timeKey1 = new TimeKey(1, 1);
			
			TestValue testValue2 = new TestValue("Rick", 5);
			TimeKey timeKey2 = new TimeKey(5, 5);
			
			Path dataDirectoryPath = timeCollection.getPath().resolve("0");
			Path fileChunkPath1 = dataDirectoryPath.resolve("0/0/0/1_1");
			Path fileChunkPath2 = dataDirectoryPath.resolve("0/0/0/5_5");
			
			timeCollection.insert(timeKey1, testValue1);
			timeCollection.insert(timeKey2, testValue2);
			
			assertEquals(testValue1, timeCollection.get(timeKey1));
			assertEquals(testValue2, timeCollection.get(timeKey2));
			assertTrue(Files.exists(fileChunkPath1));
			assertTrue(Files.exists(fileChunkPath2));
			assertTrue(Files.exists(dataDirectoryPath));
			
			timeCollection.delete(timeKey1);
			
			assertEquals(null, timeCollection.get(timeKey1));
			assertFalse(Files.exists(fileChunkPath1));
			assertTrue(Files.exists(fileChunkPath2));
			assertTrue(Files.exists(dataDirectoryPath));
			assertEquals(testValue2, timeCollection.get(timeKey2));
			assertTrue(Files.exists(timeCollection.getPath()));
		}
	}
	@Test
	public void test_activeTimeIndexEmptyAfterRecordCompleted() throws IOException, BlueDbException, InterruptedException, ExecutionException {
		try (BlueDbOnDiskWrapper dbWrapper = new BlueDbOnDiskWrapper(StartupOption.EncryptionDisabled, BlueCollectionVersion.VERSION_2)) {
			
			ReadWriteTimeCollectionOnDisk<TestValue> timeCollection = dbWrapper.getTimeCollection();
			
			TestValue testValue = new TestValue("Bob", 3);
			ActiveTimeKey activeKey = new ActiveTimeKey(1, 1);
			TimeFrameKey completedKey = new TimeFrameKey(1, 1, 3600000);
			
			Path indexPath = timeCollection.getPath().resolve(".index/active-record-times-index");
			Path dataDirectoryPath = indexPath.resolve("0");
			Path fileChunkPath = dataDirectoryPath.resolve("0/0/0/1_1");
			
			timeCollection.insert(activeKey, testValue);
			
			assertEquals(testValue, timeCollection.get(activeKey));
			assertEquals(testValue, timeCollection.get(completedKey));
			assertTrue(Files.exists(fileChunkPath));
			assertTrue(Files.exists(dataDirectoryPath));
			
			testValue.addCupcake();
			timeCollection.update(completedKey, valueToUpdate -> valueToUpdate.addCupcake());
			
			assertEquals(testValue, timeCollection.get(activeKey));
			assertEquals(testValue, timeCollection.get(completedKey));
			assertFalse(Files.exists(fileChunkPath));

			
			timeCollection.getRollupScheduler().forceScheduleRollups(true); //Rollups should delete the empty directories
			
			assertFalse(Files.exists(dataDirectoryPath));
			assertTrue(Files.exists(timeCollection.getPath()));
			assertTrue(Files.exists(indexPath));
		}
	}
}
