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
import org.bluedb.api.keys.LongTimeKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.StreamUtils;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.TestSegmentRangeCalculator;
import org.bluedb.disk.collection.index.ReadableIndexOnDiskMocker;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.serialization.BlueEntity;
import org.junit.Test;

public class OnDiskLongIndexConditionTest {
	
	private static final String INDEX_NAME = "index-name";
	private static final Path INDEX_PATH = Paths.get(INDEX_NAME);
	
	private ReadableIndexOnDiskMocker<LongKey, TestValue> longIndexMocker = new ReadableIndexOnDiskMocker<>();
	private OnDiskLongIndexCondition<TestValue> longIndexCondition = new OnDiskLongIndexCondition<>(longIndexMocker.getIndex());
	
	private ReadableIndexOnDiskMocker<LongTimeKey, TestValue> longTimeIndexMocker = new ReadableIndexOnDiskMocker<>();
	private OnDiskLongIndexCondition<TestValue> longTimeIndexCondition = new OnDiskLongIndexCondition<>(longTimeIndexMocker.getIndex());
	
	private TestSegmentRangeCalculator segmentRangeCalculator = new TestSegmentRangeCalculator(10);
	
	private BlueEntity<TestValue> entity1 = new BlueEntity<TestValue>(new TimeKey(1, 6), new TestValue("name1", 6));
	private BlueEntity<TestValue> entity2 = new BlueEntity<TestValue>(new TimeKey(2, 10), new TestValue("name2", 10));
	private BlueEntity<TestValue> entity3 = new BlueEntity<TestValue>(new TimeKey(3, 15), new TestValue("name3", 15));
	private BlueEntity<TestValue> entity4 = new BlueEntity<TestValue>(new TimeKey(4, 18), new TestValue("name4", 18));
	private BlueEntity<TestValue> entity5 = new BlueEntity<TestValue>(new TimeKey(5, 24), new TestValue("name5", 24));
	private BlueEntity<TestValue> entity6 = new BlueEntity<TestValue>(new TimeKey(6, 29), new TestValue("name6", 24));
	private BlueEntity<TestValue> entity7 = new BlueEntity<TestValue>(new TimeKey(7, 35), new TestValue("name7", 29));
	private BlueEntity<TestValue> entity8 = new BlueEntity<TestValue>(new TimeKey(8, 78), new TestValue("name8", 55));
	private BlueEntity<TestValue> entity9 = new BlueEntity<TestValue>(new TimeKey(9, 90), new TestValue("name9", -1)); //Becomes a null index value
	private BlueEntity<TestValue> entity10 = new BlueEntity<TestValue>(new TimeKey(10, 110), new TestValue("name10", Integer.MIN_VALUE)); //Becomes an index value of the wrong type

	private List<BlueEntity<TestValue>> allEntities = Arrays.asList(entity1, entity2, entity3, entity4, entity5, entity6, entity7, entity8, entity9, entity10);
	
	public OnDiskLongIndexConditionTest() {
		longIndexMocker.setIndexName(INDEX_NAME);
		longIndexMocker.setIndexedCollectionType(TestValue.class);
		longIndexMocker.setIndexKeyType(LongKey.class);
		longIndexMocker.setIndexPath(INDEX_PATH);
		
		longIndexMocker.setIndexKeyToIndexSegmentRangeMapper(valueKey -> segmentRangeCalculator.calculateRange(valueKey.getGroupingNumber()));
		longIndexMocker.setValueKeyToCollectionSegmentRangeMapper(valueKey -> segmentRangeCalculator.calculateRange(valueKey.getGroupingNumber()));
		
		longIndexMocker.setIndexExtractor(entity -> {
			ValueKey indexKey = toIndexKey(entity);
			return indexKey == null ? new LinkedList<>() : new LinkedList<>(Arrays.asList(indexKey));	
		});
		longIndexMocker.setEntities(new LinkedList<>(allEntities));
		
		longTimeIndexMocker.setIndexName(INDEX_NAME);
		longTimeIndexMocker.setIndexedCollectionType(TestValue.class);
		longTimeIndexMocker.setIndexKeyType(LongTimeKey.class);
		longTimeIndexMocker.setIndexPath(INDEX_PATH);
		
		longTimeIndexMocker.setIndexKeyToIndexSegmentRangeMapper(valueKey -> segmentRangeCalculator.calculateRange(valueKey.getGroupingNumber()));
		longTimeIndexMocker.setValueKeyToCollectionSegmentRangeMapper(valueKey -> segmentRangeCalculator.calculateRange(valueKey.getGroupingNumber()));
		
		longTimeIndexMocker.setIndexExtractor(entity -> {
			ValueKey indexKey = toIndexKey(entity);
			return indexKey == null ? new LinkedList<>() : new LinkedList<>(Arrays.asList(indexKey));	
		});
		longTimeIndexMocker.setEntities(new LinkedList<>(allEntities));
	}

	@Test
	public void test_simpleGetters() {
		assertEquals(INDEX_NAME, longIndexCondition.getIndexName());
		assertEquals(TestValue.class, longIndexCondition.getIndexedCollectionType());
		assertEquals(LongKey.class, longIndexCondition.getIndexKeyType());
		assertEquals(INDEX_PATH, longIndexCondition.getIndexPath());
		
	}
	
	@Test
	public void test_extractIndexValueFromKey() {
		assertNull(longIndexCondition.extractIndexValueFromKey(null));
		assertNull(longTimeIndexCondition.extractIndexValueFromKey(null));
		
		assertNull(longIndexCondition.extractIndexValueFromKey(new StringKey("My String")));
		assertNull(longTimeIndexCondition.extractIndexValueFromKey(new StringKey("My String")));
		
		for(BlueEntity<TestValue> entity : allEntities) {
			ValueKey indexKey = toIndexKey(entity);
			if(indexKey instanceof LongKey) {
				Long expectedIndexValue = indexKey == null ? null : ((LongKey)indexKey).getId();
				assertEquals(expectedIndexValue, longIndexCondition.extractIndexValueFromKey(indexKey));
			} else {
				assertNull(longIndexCondition.extractIndexValueFromKey(indexKey));
				assertNull(longTimeIndexCondition.extractIndexValueFromKey(indexKey));
			}
		}

		assertEquals(1L, (long) longIndexCondition.extractIndexValueFromKey(new LongTimeKey(1L)));
	}
	
	@Test
	public void test_createKeyForIndexValue() {
		assertEquals(new LongKey(1), longIndexCondition.createKeyForIndexValue(1L));
		assertEquals(new LongTimeKey(1), longTimeIndexCondition.createKeyForIndexValue(1L));
	}
	
	@Test
	public void test_extractIndexKeysFromEntity() {
		for(BlueEntity<TestValue> entity : allEntities) {
			assertEquals(toIndexKeyList(entity), longIndexCondition.extractIndexKeysFromEntity(entity));
		}
	}
	
	@Test
	public void test_conditionConfiguration_exceptionsThrownForPassingNullIn() {
		try {
			longIndexCondition.isEqualTo(null);
			fail();
		} catch(InvalidParameterException e) { }
		
		try {
			longIndexCondition.isIn((Set<Long>)null);
			fail();
		} catch(InvalidParameterException e) { }
		
		try {
			longIndexCondition.isIn((BlueSimpleSet<Long>)null);
			fail();
		} catch(InvalidParameterException e) { }
		
		try {
			longIndexCondition.meets(null);
			fail();
		} catch(InvalidParameterException e) { }
	}
	
	@Test
	public void test_conditionConfiguration_exceptionsThrownForAddingConditionsAfterSettingEqualsCondition() {
		longIndexCondition.isEqualTo(10L);
		
		try {
			longIndexCondition.isIn(new BlueSimpleInMemorySet<Long>(new HashSet<>()));
			fail();
		} catch(IllegalStateException e) { }
		
		try {
			longIndexCondition.isIn(new HashSet<>());
			fail();
		} catch(IllegalStateException e) { }
		
		try {
			longIndexCondition.meets(indexedLong -> false);
			fail();
		} catch(IllegalStateException e) { }
		
		try {
			longIndexCondition.isInRange(0, 1);
			fail();
		} catch(IllegalStateException e) { }
		
		try {
			longIndexCondition.isLessThan(10);
			fail();
		} catch(IllegalStateException e) { }
		
		try {
			longIndexCondition.isLessThanOrEqualTo(10);
			fail();
		} catch(IllegalStateException e) { }
		
		try {
			longIndexCondition.isGreaterThan(10);
			fail();
		} catch(IllegalStateException e) { }
		
		try {
			longIndexCondition.isGreaterThanOrEqualTo(10);
			fail();
		} catch(IllegalStateException e) { }
	}
	
	@Test
	public void test_allEntitiesContainingIndexValueMatchByDefault() {
		assertTestMethod(entity1, entity2, entity3, entity4, entity5, entity6, entity7, entity8);
		assertEquals(toIncludedSegmentRangeInfo(entity1, entity2, entity3, entity4, entity5, entity6, entity7, entity8), longIndexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isIn() {
		longIndexCondition.isIn(new HashSet<Long>(Arrays.asList(10L, 24L, 29L)));
		
		assertTestMethod(entity2, entity5, entity6, entity7);
		assertEquals(toIncludedSegmentRangeInfo(entity2, entity5, entity6, entity7), longIndexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_meets() {
		HashSet<Long> valuesToMatch = new HashSet<Long>(Arrays.asList(10L, 24L, 29L));
		longIndexCondition.meets(value -> valuesToMatch.contains(value));
		
		assertTestMethod(entity2, entity5, entity6, entity7);
		assertEquals(toIncludedSegmentRangeInfo(entity2, entity5, entity6, entity7), longIndexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isInRange() {
		longIndexCondition.isInRange(10, 29);
		
		assertTestMethod(entity2, entity3, entity4, entity5, entity6, entity7);
		assertEquals(toIncludedSegmentRangeInfo(entity2, entity3, entity4, entity5, entity6, entity7), longIndexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isEqualTo() {
		longIndexCondition.isEqualTo(24L);
		
		assertTestMethod(entity5, entity6);
		assertEquals(toIncludedSegmentRangeInfo(entity5, entity6), longIndexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isLessThan() {
		longIndexCondition.isLessThan(24);
		
		assertTestMethod(entity1, entity2, entity3, entity4);
		assertEquals(toIncludedSegmentRangeInfo(entity1, entity2, entity3, entity4), longIndexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isLessThanOrEqualTo() {
		longIndexCondition.isLessThanOrEqualTo(24);
		
		assertTestMethod(entity1, entity2, entity3, entity4, entity5, entity6);
		assertEquals(toIncludedSegmentRangeInfo(entity1, entity2, entity3, entity4, entity5, entity6), longIndexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isGreaterThan() {
		longIndexCondition.isGreaterThan(24);
		
		assertTestMethod(entity7, entity8);
		assertEquals(toIncludedSegmentRangeInfo(entity7, entity8), longIndexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isGreaterThanOrEqualTo() {
		longIndexCondition.isGreaterThanOrEqualTo(24);
		
		assertTestMethod(entity5, entity6, entity7, entity8);
		assertEquals(toIncludedSegmentRangeInfo(entity5, entity6, entity7, entity8), longIndexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isGreaterThanANDisLessThan() {
		longIndexCondition.isGreaterThan(10).isLessThan(24);
		
		assertTestMethod(entity3, entity4);
		assertEquals(toIncludedSegmentRangeInfo(entity3, entity4), longIndexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
	}
	
	private ValueKey toIndexKey(BlueEntity<TestValue> collectionEntity) {
		return toIndexKey(collectionEntity.getValue());
	}
	
	private ValueKey toIndexKey(TestValue collectionValue) {
		if(collectionValue.getCupcakes() == Integer.MIN_VALUE) {
			return new StringKey("Incorrect Index Key Type");
		} else if(collectionValue.getCupcakes() < 0) {
			return null;
		} else {
			return new LongKey(collectionValue.getCupcakes());
		}
	}

	@SafeVarargs
	private final List<ValueKey> toIndexKeyList(BlueEntity<TestValue>...entities) {
		return StreamUtils.stream(entities)
			.map(this::toIndexKey)
			.filter(Objects::nonNull)
			.collect(Collectors.toCollection(LinkedList::new));
	}

	@SafeVarargs
	private final IncludedSegmentRangeInfo toIncludedSegmentRangeInfo(BlueEntity<TestValue>...entities) {
		IncludedSegmentRangeInfo includedSegmentRangeInfo = new IncludedSegmentRangeInfo();
		
		StreamUtils.stream(entities)
			.forEach(entity -> {
				Range segmentRange = segmentRangeCalculator.calculateRange(entity.getKey().getGroupingNumber());
				includedSegmentRangeInfo.addIncludedSegmentRangeInfo(segmentRange, entity.getKey().getGroupingNumber());
			});
		
		return includedSegmentRangeInfo;
	}

	@SafeVarargs
	private final void assertTestMethod(BlueEntity<TestValue>...entitiesThatShouldMatch) {
		HashSet<BlueEntity<TestValue>> entitySetThatShouldMatch = StreamUtils.stream(entitiesThatShouldMatch)
			.collect(Collectors.toCollection(HashSet::new));
		
		for(BlueEntity<TestValue> entity : allEntities) {
			boolean shouldMatch = entitySetThatShouldMatch.contains(entity);
			String failureMessage = "Expected entity " + entity + " to " + (shouldMatch ? "match" : "not match");
			assertEquals(failureMessage, shouldMatch, longIndexCondition.test(entity));
		}
	}
	
}
