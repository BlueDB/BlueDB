package io.bluedb.disk.backup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.BlueDbOnDisk;
import io.bluedb.disk.BlueDbOnDiskBuilder;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionOnDisk;

public class BackupTaskTest extends BlueDbDiskTestBase {

	@Test
	public void test() {
		try {
			BlueKey key1At1 = createKey(1, 1);
			TestValue value1 = createValue("Anna");
			getCollection().insert(key1At1, value1);
			List<BlueCollectionOnDisk<?>> collectionsToBackup = Arrays.asList(getCollection());

			Path backedUpPath = Files.createTempDirectory(this.getClass().getSimpleName());
			BackupTask backupTask = new BackupTask(db(), backedUpPath);
			backupTask.backup(collectionsToBackup);

			BlueDbOnDisk restoredDb = new BlueDbOnDiskBuilder().setPath(backedUpPath).build();
			BlueCollectionOnDisk<TestValue> restoredCollection = (BlueCollectionOnDisk<TestValue>) restoredDb.getCollection(TestValue.class, "testing");
			assertTrue(restoredCollection.contains(key1At1));
			assertEquals(value1, restoredCollection.get(key1At1));
			Long restoredMaxLong = restoredCollection.getMaxLongId();
			assertNotNull(restoredMaxLong);
			assertEquals(getCollection().getMaxLongId().longValue(), restoredMaxLong.longValue());

		} catch (IOException | BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

}
