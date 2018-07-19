package io.bluedb.disk.collection;

import org.junit.Test;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.BlueDbDiskTestBase;

public class CollectionMetaDataTest extends BlueDbDiskTestBase {

	private CollectionMetaData metaData;
	
	@Override
	public void setUp() throws Exception {
		super.setUp();
		metaData = getCollection().getMetaData();
	}

	@Test
	public void test_getMaxLong() {
		try {
			assertNull(metaData.getMaxLong());
			metaData.updateMaxLong(1);
			assertEquals(1, 1L);
			metaData.updateMaxLong(3);
			assertEquals(3, metaData.getMaxLong().longValue());
			metaData.updateMaxLong(2);
			assertEquals(3, metaData.getMaxLong().longValue());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_getMaxInteger() {
		try {
			assertNull(metaData.getMaxInteger());
			metaData.updateMaxInteger(1);
			assertEquals(1, 1L);
			metaData.updateMaxInteger(3);
			assertEquals(3, metaData.getMaxInteger().longValue());
			metaData.updateMaxInteger(2);
			assertEquals(3, metaData.getMaxInteger().longValue());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_updateMaxLong() {
		try {
			assertNull(metaData.getMaxLong());
			metaData.updateMaxLong(1);
			assertEquals(1, 1L);
			metaData.updateMaxLong(3);
			assertEquals(3, metaData.getMaxLong().longValue());
			metaData.updateMaxLong(2);
			assertEquals(3, metaData.getMaxLong().longValue());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_updateMaxInteger() {
		try {
			assertNull(metaData.getMaxInteger());
			metaData.updateMaxInteger(1);
			assertEquals(1, 1L);
			metaData.updateMaxInteger(3);
			assertEquals(3, metaData.getMaxInteger().longValue());
			metaData.updateMaxInteger(2);
			assertEquals(3, metaData.getMaxInteger().longValue());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}
}
