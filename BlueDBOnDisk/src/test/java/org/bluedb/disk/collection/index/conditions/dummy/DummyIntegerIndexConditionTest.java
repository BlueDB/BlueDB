package org.bluedb.disk.collection.index.conditions.dummy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;

import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.serialization.BlueEntity;
import org.junit.Test;

public class DummyIntegerIndexConditionTest {
	
	private BlueEntity<TestValue> entity1 = new BlueEntity<TestValue>(new TimeKey(1, 1), new TestValue("name1", 1));
	private BlueEntity<TestValue> entity2 = new BlueEntity<TestValue>(new TimeKey(2, 2), new TestValue("name2", 2));
	
	private DummyIntegerIndexCondition<TestValue> indexCondition = new DummyIntegerIndexCondition<>(TestValue.class);

	@Test
	public void testDefaultGetters() {
		assertEquals(TestValue.class, indexCondition.getIndexedCollectionType());
		assertEquals(IntegerKey.class, indexCondition.getIndexKeyType());
		assertEquals(OnDiskDummyIndexCondition.DUMMY_INDEX_NAME, indexCondition.getIndexName());
		assertNull(indexCondition.getIndexPath());
		assertTrue(indexCondition.getSegmentRangeInfoToIncludeInCollectionQuery().isEmpty());
	}

	@Test
	public void testTest_returnsFalseWithoutAnyConditions() {
		assertFalse(indexCondition.test(null));
		assertFalse(indexCondition.test(entity1));
		assertFalse(indexCondition.test(entity2));
	}

	@Test
	public void testTest_dummyMethodsDoNotThrowExceptions() {
		indexCondition.isEqualTo(null);
		indexCondition.isIn(new HashSet<>());
		indexCondition.meets(intValue -> intValue == 0);
		indexCondition.isInRange(0, 1);
		indexCondition.isLessThan(0);
		indexCondition.isLessThanOrEqualTo(0);
		indexCondition.isGreaterThan(0);
		indexCondition.isGreaterThanOrEqualTo(0);
		
		assertFalse(indexCondition.test(null));
		assertFalse(indexCondition.test(entity1));
		assertFalse(indexCondition.test(entity2));
	}
	
}
