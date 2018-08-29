package io.bluedb.disk.collection.index;

import static org.junit.Assert.*;
import java.nio.file.Path;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.IntegerKey;
import io.bluedb.api.keys.LongKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionOnDisk;

public class BlueIndexOnDiskTest extends BlueDbDiskTestBase {

	@Test
	public void test_getKeys() throws Exception {
		// TODO test ranges?
		BlueCollectionOnDisk<TestValue> collection = getTimeCollection();
		TestValue value = new TestValue("Joe", 3);
		TimeKey timeKey = createTimeKey(1, value);
		IntegerKey integerKey = new IntegerKey(3);
		Path indexPath = createTempFolder().toPath();

		collection.createIndex("test_index", IntegerKey.class, new TestRetrievalKeyExtractor());
		BlueIndexOnDisk<IntegerKey, TestValue> index = (BlueIndexOnDisk<IntegerKey, TestValue>) collection.getIndex("test_index", IntegerKey.class);

		assertFalse(index.getKeys(integerKey).contains(timeKey));
		collection.insert(timeKey,  value);
		assertEquals(1, collection.query().count());
		assertTrue(index.getKeys(integerKey).contains(timeKey));
	}
}
