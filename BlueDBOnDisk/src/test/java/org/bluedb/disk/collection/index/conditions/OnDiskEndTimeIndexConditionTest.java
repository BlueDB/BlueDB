package org.bluedb.disk.collection.index.conditions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.bluedb.api.datastructures.BlueSimpleInMemorySet;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.StreamUtils;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.TestSegmentRangeCalculator;
import org.bluedb.disk.collection.index.ReadableIndexOnDiskMocker;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.serialization.BlueEntity;
import org.junit.Test;

public class OnDiskEndTimeIndexConditionTest {
	
	private static final String INDEX_NAME = "index-name";
	private static final Path INDEX_PATH = Paths.get(INDEX_NAME);
	
	private ReadableIndexOnDiskMocker<LongKey, TestValue> indexMocker = new ReadableIndexOnDiskMocker<>();
	private OnDiskEndTimeIndexCondition<TestValue> indexCondition = new OnDiskEndTimeIndexCondition<>(indexMocker.getIndex());
	private TestSegmentRangeCalculator segmentRangeCalculator = new TestSegmentRangeCalculator(10);
	
	private BlueEntity<TestValue> entity1 = new BlueEntity<TestValue>(new TimeFrameKey(1, 6, 20), new TestValue("name1", 6));
	private BlueEntity<TestValue> entity2 = new BlueEntity<TestValue>(new TimeFrameKey(2, 10, 25), new TestValue("name2", 10));
	private BlueEntity<TestValue> entity3 = new BlueEntity<TestValue>(new TimeFrameKey(3, 15, 22), new TestValue("name3", 15));
	private BlueEntity<TestValue> entity4 = new BlueEntity<TestValue>(new TimeFrameKey(4, 18, 30), new TestValue("name4", 18));
	private BlueEntity<TestValue> entity5 = new BlueEntity<TestValue>(new TimeFrameKey(5, 24, 35), new TestValue("name5", 24));
	private BlueEntity<TestValue> entity6 = new BlueEntity<TestValue>(new TimeFrameKey(6, 29, 55), new TestValue("name6", 24));
	private BlueEntity<TestValue> entity7 = new BlueEntity<TestValue>(new TimeFrameKey(7, 35, 55), new TestValue("name7", 29));
	private BlueEntity<TestValue> entity8 = new BlueEntity<TestValue>(new TimeFrameKey(8, 78, 80), new TestValue("name8", 55));
	private BlueEntity<TestValue> entity9 = new BlueEntity<TestValue>(new LongKey(9), new TestValue("name9", -1)); //Becomes a null index value

	private List<BlueEntity<TestValue>> allEntities = Arrays.asList(entity1, entity2, entity3, entity4, entity5, entity6, entity7, entity8, entity9);
	
	public OnDiskEndTimeIndexConditionTest() {
		indexMocker.setIndexName(INDEX_NAME);
		indexMocker.setIndexedCollectionType(TestValue.class);
		indexMocker.setIndexKeyType(LongKey.class);
		indexMocker.setIndexPath(INDEX_PATH);
		
		indexMocker.setIndexKeyToIndexSegmentRangeMapper(valueKey -> segmentRangeCalculator.calculateRange(valueKey.getGroupingNumber()));
		indexMocker.setValueKeyToCollectionSegmentRangeMapper(valueKey -> segmentRangeCalculator.calculateRange(valueKey.getGroupingNumber()));
		
		indexMocker.setIndexExtractor(entity -> {
			ValueKey indexKey = toIndexKey(entity);
			return indexKey == null ? new LinkedList<>() : new LinkedList<>(Arrays.asList(indexKey));	
		});
		
		indexMocker.setEntities(new LinkedList<>(allEntities));
	}

	@Test
	public void test_simpleGetters() {
		assertEquals(INDEX_NAME, indexCondition.getIndexName());
		assertEquals(TestValue.class, indexCondition.getIndexedCollectionType());
		assertEquals(LongKey.class, indexCondition.getIndexKeyType());
		assertEquals(INDEX_PATH, indexCondition.getIndexPath());
		
	}
	
	@Test
	public void test_extractIndexValueFromKey() {
		assertNull(indexCondition.extractIndexValueFromKey(null));
		assertNull(indexCondition.extractIndexValueFromKey(new StringKey("My String")));
		
		for(BlueEntity<TestValue> entity : allEntities) {
			ValueKey indexKey = toIndexKey(entity);
			if(indexKey instanceof LongKey) {
				Long expectedIndexValue = indexKey == null ? null : ((LongKey)indexKey).getId();
				assertEquals(expectedIndexValue, indexCondition.extractIndexValueFromKey(indexKey));
			} else {
				assertNull(indexCondition.extractIndexValueFromKey(indexKey));
			}
		}
	}
	
	@Test
	public void test_extractIndexKeysFromEntity() {
		for(BlueEntity<TestValue> entity : allEntities) {
			assertEquals(toIndexKeyList(entity), indexCondition.extractIndexKeysFromEntity(entity));
		}
	}
	
	@Test
	public void test_conditionConfiguration_exceptionsThrownForPassingNullIn() {
		try {
			indexCondition.isEqualTo(null);
			fail();
		} catch(InvalidParameterException e) { }
		
		try {
			indexCondition.isIn((Set<Long>)null);
			fail();
		} catch(InvalidParameterException e) { }
		
		try {
			indexCondition.isIn((BlueSimpleSet<Long>)null);
			fail();
		} catch(InvalidParameterException e) { }
		
		try {
			indexCondition.meets(null);
			fail();
		} catch(InvalidParameterException e) { }
	}
	
	@Test
	public void test_conditionConfiguration_exceptionsThrownForAddingConditionsAfterSettingEqualsCondition() {
		indexCondition.isEqualTo(10L);
		
		try {
			indexCondition.isIn(new BlueSimpleInMemorySet<Long>(new HashSet<>()));
			fail();
		} catch(IllegalStateException e) { }
		
		try {
			indexCondition.isIn(new HashSet<>());
			fail();
		} catch(IllegalStateException e) { }
		
		try {
			indexCondition.meets(indexedLong -> false);
			fail();
		} catch(IllegalStateException e) { }
		
		try {
			indexCondition.isLessThan(10);
			fail();
		} catch(IllegalStateException e) { }
		
		try {
			indexCondition.isLessThanOrEqualTo(10);
			fail();
		} catch(IllegalStateException e) { }
		
		try {
			indexCondition.isGreaterThan(10);
			fail();
		} catch(IllegalStateException e) { }
		
		try {
			indexCondition.isGreaterThanOrEqualTo(10);
			fail();
		} catch(IllegalStateException e) { }
	}
	
	@Test
	public void test_allEntitiesContainingIndexValueMatchByDefault() {
		assertTestMethod(entity1, entity2, entity3, entity4, entity5, entity6, entity7, entity8);
		assertEquals(toRangeSet(entity1, entity2, entity3, entity4, entity5, entity6, entity7, entity8), indexCondition.getSegmentRangesToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isIn() {
		indexCondition.isIn(new HashSet<Long>(Arrays.asList(25L, 55L)));
		
		assertTestMethod(entity2, entity6, entity7);
		assertEquals(toRangeSet(entity2, entity6, entity7), indexCondition.getSegmentRangesToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_meets() {
		HashSet<Long> valuesToMatch = new HashSet<Long>(Arrays.asList(25L, 55L));
		indexCondition.meets(value -> valuesToMatch.contains(value));
		
		assertTestMethod(entity2, entity6, entity7);
		assertEquals(toRangeSet(entity2, entity6, entity7), indexCondition.getSegmentRangesToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isEqualTo() {
		indexCondition.isEqualTo(35L);
		
		assertTestMethod(entity5);
		assertEquals(toRangeSet(entity5), indexCondition.getSegmentRangesToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isLessThan() {
		indexCondition.isLessThan(35);
		
		assertTestMethod(entity1, entity2, entity3, entity4);
		assertEquals(toRangeSet(entity1, entity2, entity3, entity4), indexCondition.getSegmentRangesToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isLessThanOrEqualTo() {
		indexCondition.isLessThanOrEqualTo(35);
		
		assertTestMethod(entity1, entity2, entity3, entity4, entity5);
		assertEquals(toRangeSet(entity1, entity2, entity3, entity4, entity5), indexCondition.getSegmentRangesToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isGreaterThan() {
		indexCondition.isGreaterThan(35);
		
		assertTestMethod(entity6, entity7, entity8);
		assertEquals(toRangeSet(entity6, entity7, entity8), indexCondition.getSegmentRangesToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isGreaterThanOrEqualTo() {
		indexCondition.isGreaterThanOrEqualTo(35);
		
		assertTestMethod(entity5, entity6, entity7, entity8);
		assertEquals(toRangeSet(entity5, entity6, entity7, entity8), indexCondition.getSegmentRangesToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isGreaterThanANDisLessThan() {
		indexCondition.isGreaterThan(20).isLessThan(35);
		
		assertTestMethod(entity2, entity3, entity4);
		assertEquals(toRangeSet(entity2, entity3, entity4), indexCondition.getSegmentRangesToIncludeInCollectionQuery());
	}
	
	private ValueKey toIndexKey(BlueEntity<TestValue> collectionEntity) {
		return indexCondition.extractEndTimeIndexKeyFromEntity(collectionEntity);
	}

	@SafeVarargs
	private final List<ValueKey> toIndexKeyList(BlueEntity<TestValue>...entities) {
		return StreamUtils.stream(entities)
			.map(this::toIndexKey)
			.filter(Objects::nonNull)
			.collect(Collectors.toCollection(LinkedList::new));
	}

	@SafeVarargs
	private final Set<Range> toRangeSet(BlueEntity<TestValue>...entities) {
		return StreamUtils.stream(entities)
			.map(entity -> segmentRangeCalculator.calculateRange(entity.getKey().getGroupingNumber()))
			.collect(Collectors.toCollection(HashSet::new));
	}

	@SafeVarargs
	private final void assertTestMethod(BlueEntity<TestValue>...entitiesThatShouldMatch) {
		HashSet<BlueEntity<TestValue>> entitySetThatShouldMatch = StreamUtils.stream(entitiesThatShouldMatch)
			.collect(Collectors.toCollection(HashSet::new));
		
		for(BlueEntity<TestValue> entity : allEntities) {
			boolean shouldMatch = entitySetThatShouldMatch.contains(entity);
			String failureMessage = "Expected entity " + entity + " to " + (shouldMatch ? "match" : "not match");
			assertEquals(failureMessage, shouldMatch, indexCondition.test(entity));
		}
	}
	
}
