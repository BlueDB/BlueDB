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

import org.bluedb.api.CloseableIterator;
import org.bluedb.api.datastructures.BlueSimpleInMemorySet;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.IntegerKey;
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

public class OnDiskIntegerIndexConditionTest {
	
	private static final String INDEX_NAME = "index-name";
	private static final Path INDEX_PATH = Paths.get(INDEX_NAME);
	
	private ReadableIndexOnDiskMocker<IntegerKey, TestValue> indexMocker = new ReadableIndexOnDiskMocker<>();
	private OnDiskIntegerIndexCondition<TestValue> indexCondition = new OnDiskIntegerIndexCondition<>(indexMocker.getIndex());
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
	
	public OnDiskIntegerIndexConditionTest() {
		indexMocker.setIndexName(INDEX_NAME);
		indexMocker.setIndexedCollectionType(TestValue.class);
		indexMocker.setIndexKeyType(IntegerKey.class);
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
		assertEquals(IntegerKey.class, indexCondition.getIndexKeyType());
		assertEquals(INDEX_PATH, indexCondition.getIndexPath());
		
	}
	
	@Test
	public void test_extractIndexValueFromKey() {
		assertNull(indexCondition.extractIndexValueFromKey(null));
		assertNull(indexCondition.extractIndexValueFromKey(new StringKey("My String")));
		
		for(BlueEntity<TestValue> entity : allEntities) {
			ValueKey indexKey = toIndexKey(entity);
			if(indexKey instanceof IntegerKey) {
				Integer expectedIndexValue = indexKey == null ? null : ((IntegerKey)indexKey).getId();
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
			indexCondition.isIn((Set<Integer>)null);
			fail();
		} catch(InvalidParameterException e) { }
		
		try {
			indexCondition.isIn((BlueSimpleSet<Integer>)null);
			fail();
		} catch(InvalidParameterException e) { }
		
		try {
			indexCondition.meets(null);
			fail();
		} catch(InvalidParameterException e) { }
	}
	
	@Test
	public void test_conditionConfiguration_exceptionsThrownForAddingConditionsAfterSettingEqualsCondition() {
		indexCondition.isEqualTo(10);
		
		try {
			indexCondition.isIn(new BlueSimpleInMemorySet<Integer>(new HashSet<>()));
			fail();
		} catch(IllegalStateException e) { }
		
		try {
			indexCondition.isIn(new HashSet<>());
			fail();
		} catch(IllegalStateException e) { }
		
		try {
			indexCondition.meets(indexedInt -> false);
			fail();
		} catch(IllegalStateException e) { }
		
		try {
			indexCondition.isInRange(0, 1);
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
		assertMatchingValueKeys(entity1, entity2, entity3, entity4, entity5, entity6, entity7, entity8);
		assertEquals(toIncludedSegmentRangeInfo(entity1, entity2, entity3, entity4, entity5, entity6, entity7, entity8), indexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_allValueKeysMatchWhenNonIndexCompositeKeysAreReturnedByIndex() {
		indexMocker.setEntitiesToReturnNonCompositeIndexKeys(new LinkedList<>(allEntities));
		assertMatchingValueKeys(entity1, entity2, entity3, entity4, entity5, entity6, entity7, entity8);
	}
	
	@Test
	public void test_isIn() {
		indexCondition.isIn(new HashSet<Integer>(Arrays.asList(10, 24, 29)));
		
		assertTestMethod(entity2, entity5, entity6, entity7);
		assertMatchingValueKeys(entity2, entity5, entity6, entity7);
		assertEquals(toIncludedSegmentRangeInfo(entity2, entity5, entity6, entity7), indexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_meets() {
		HashSet<Integer> valuesToMatch = new HashSet<Integer>(Arrays.asList(10, 24, 29));
		indexCondition.meets(value -> valuesToMatch.contains(value));
		
		assertTestMethod(entity2, entity5, entity6, entity7);
		assertMatchingValueKeys(entity2, entity5, entity6, entity7);
		assertEquals(toIncludedSegmentRangeInfo(entity2, entity5, entity6, entity7), indexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isInRange() {
		indexCondition.isInRange(10, 29);
		
		assertTestMethod(entity2, entity3, entity4, entity5, entity6, entity7);
		assertMatchingValueKeys(entity2, entity3, entity4, entity5, entity6, entity7);
		assertEquals(toIncludedSegmentRangeInfo(entity2, entity3, entity4, entity5, entity6, entity7), indexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isEqualTo() {
		indexCondition.isEqualTo(24);
		
		assertTestMethod(entity5, entity6);
		assertMatchingValueKeys(entity5, entity6);
		assertEquals(toIncludedSegmentRangeInfo(entity5, entity6), indexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isLessThan() {
		indexCondition.isLessThan(24);
		
		assertTestMethod(entity1, entity2, entity3, entity4);
		assertMatchingValueKeys(entity1, entity2, entity3, entity4);
		assertEquals(toIncludedSegmentRangeInfo(entity1, entity2, entity3, entity4), indexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isLessThanOrEqualTo() {
		indexCondition.isLessThanOrEqualTo(24);
		
		assertTestMethod(entity1, entity2, entity3, entity4, entity5, entity6);
		assertMatchingValueKeys(entity1, entity2, entity3, entity4, entity5, entity6);
		assertEquals(toIncludedSegmentRangeInfo(entity1, entity2, entity3, entity4, entity5, entity6), indexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isGreaterThan() {
		indexCondition.isGreaterThan(24);
		
		assertTestMethod(entity7, entity8);
		assertMatchingValueKeys(entity7, entity8);
		assertEquals(toIncludedSegmentRangeInfo(entity7, entity8), indexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isGreaterThanOrEqualTo() {
		indexCondition.isGreaterThanOrEqualTo(24);
		
		assertTestMethod(entity5, entity6, entity7, entity8);
		assertMatchingValueKeys(entity5, entity6, entity7, entity8);
		assertEquals(toIncludedSegmentRangeInfo(entity5, entity6, entity7, entity8), indexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
	}
	
	@Test
	public void test_isGreaterThanANDisLessThan() {
		indexCondition.isGreaterThan(10).isLessThan(24);
		
		assertTestMethod(entity3, entity4);
		assertMatchingValueKeys(entity3, entity4);
		assertEquals(toIncludedSegmentRangeInfo(entity3, entity4), indexCondition.getSegmentRangeInfoToIncludeInCollectionQuery());
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
			return new IntegerKey(collectionValue.getCupcakes());
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
			assertEquals(shouldMatch, indexCondition.test(entity));
		}
	}

	@SafeVarargs
	private final void assertMatchingValueKeys(BlueEntity<TestValue>...entitiesThatShouldMatch) {
		HashSet<BlueKey> expectedMatchingValueKeys = StreamUtils.stream(entitiesThatShouldMatch)
			.map(BlueEntity::getKey)
			.collect(Collectors.toCollection(HashSet::new));
		
		HashSet<BlueKey> actualMatchingValueKeys = new HashSet<>();
		try(CloseableIterator<BlueKey> matchingValueKeysIterator = indexCondition.getMatchingValueKeysIterator()) {
			while(matchingValueKeysIterator.hasNext()) {
				actualMatchingValueKeys.add(matchingValueKeysIterator.next());
			}
		}

		assertEquals(expectedMatchingValueKeys, actualMatchingValueKeys);
	}
	
}
