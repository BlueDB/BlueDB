package org.bluedb.disk.collection.metadata;

import static org.junit.Assert.*;

import org.bluedb.api.BlueCollectionVersion;
import org.junit.Test;

public class CollectionVersionTest {

	@Test
	public void test_utilizesDefaultTimeIndex() {
		assertFalse(BlueCollectionVersion.VERSION_1.utilizesDefaultTimeIndex());
		assertTrue(BlueCollectionVersion.VERSION_2.utilizesDefaultTimeIndex());
	}

	@Test
	public void test_getDefault() {
		assertEquals(BlueCollectionVersion.VERSION_1, BlueCollectionVersion.getDefault());
	}

}
