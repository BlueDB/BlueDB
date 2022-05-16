package org.bluedb.disk.collection.index;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.bluedb.api.CloseableIterator;
import org.bluedb.api.Condition;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.StreamUtils;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.TestSegmentRangeCalculator;
import org.bluedb.disk.collection.index.conditions.IncludedSegmentRangeInfo;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.serialization.BlueEntity;
import org.junit.Test;

public class ReadableIndexOnDiskMockerTest {
	
	private static final String INDEX_NAME = "index-name";
	private static final Path INDEX_PATH = Paths.get(INDEX_NAME);
	
	private ReadableIndexOnDiskMocker<IntegerKey, TestValue> indexMocker = new ReadableIndexOnDiskMocker<>();
	private TestSegmentRangeCalculator segmentRangeCalculator = new TestSegmentRangeCalculator(10);
	
	private Range maxRange = new Range(Long.MIN_VALUE, Long.MAX_VALUE);
	
	private BlueEntity<TestValue> entity1 = new BlueEntity<TestValue>(new TimeKey(1, 6), new TestValue("name1", 6));
	private BlueEntity<TestValue> entity2 = new BlueEntity<TestValue>(new TimeKey(2, 10), new TestValue("name2", 10));
	private BlueEntity<TestValue> entity3 = new BlueEntity<TestValue>(new TimeKey(3, 15), new TestValue("name3", 15));
	private BlueEntity<TestValue> entity4 = new BlueEntity<TestValue>(new TimeKey(4, 18), new TestValue("name4", 18));
	private BlueEntity<TestValue> entity5 = new BlueEntity<TestValue>(new TimeKey(5, 24), new TestValue("name5", 24));
	private BlueEntity<TestValue> entity6 = new BlueEntity<TestValue>(new TimeKey(6, 29), new TestValue("name6", 24));
	private BlueEntity<TestValue> entity7 = new BlueEntity<TestValue>(new TimeKey(7, 35), new TestValue("name7", 29));
	private BlueEntity<TestValue> entity8 = new BlueEntity<TestValue>(new TimeKey(8, 78), new TestValue("name8", 55));

	private List<BlueEntity<TestValue>> allEntities = Arrays.asList(entity1, entity2, entity3, entity4, entity5, entity6, entity7, entity8);
	
	public ReadableIndexOnDiskMockerTest() {
		indexMocker.setIndexName(INDEX_NAME);
		indexMocker.setIndexedCollectionType(TestValue.class);
		indexMocker.setIndexKeyType(IntegerKey.class);
		indexMocker.setIndexPath(INDEX_PATH);
		
		indexMocker.setIndexKeyToIndexSegmentRangeMapper(valueKey -> segmentRangeCalculator.calculateRange(valueKey.getGroupingNumber()));
		indexMocker.setValueKeyToCollectionSegmentRangeMapper(valueKey -> segmentRangeCalculator.calculateRange(valueKey.getGroupingNumber()));
		
		indexMocker.setIndexExtractor(entity -> new LinkedList<>(Arrays.asList(new IntegerKey(entity.getValue().getCupcakes()))));
		indexMocker.setEntities(new LinkedList<>(Arrays.asList(entity1, entity2, entity3, entity4, entity5, entity6, entity7, entity8)));
	}
	
	@Test
	public void testExtractIndexKeys() {
		for(BlueEntity<TestValue> entity : allEntities) {
			assertEquals(Arrays.asList(new IntegerKey(entity.getValue().getCupcakes())), indexMocker.getIndex().extractIndexKeys(entity));
		}
	}

	@Test
	public void testSegmentRangeCalculations() {
		assertEquals(new Range(0, 9), indexMocker.getIndex().getCollectionSegmentRangeForValueKey(entity1.getKey()));
		assertEquals(new Range(2147483640L, 2147483649L), indexMocker.getIndex().getIndexSegmentRangeForIndexKey(new IntegerKey(1)));
		
		assertEquals(new Range(10, 19), indexMocker.getIndex().getCollectionSegmentRangeForValueKey(entity2.getKey()));
		assertEquals(new Range(2147483650L, 2147483659L), indexMocker.getIndex().getIndexSegmentRangeForIndexKey(new IntegerKey(10)));
		
		assertEquals(new Range(20, 29), indexMocker.getIndex().getCollectionSegmentRangeForValueKey(entity6.getKey()));
		assertEquals(new Range(2147483670L, 2147483679L), indexMocker.getIndex().getIndexSegmentRangeForIndexKey(new IntegerKey(29)));
	}
	
	@Test
	public void testGetEntities_noCriteria() {
		assertEquals(toIndexEntityList(entity1, entity2, entity3, entity4, entity5, entity6, entity7, entity8), 
				toList(indexMocker.getIndex().getEntities(maxRange, new LinkedList<>(), new LinkedList<>(), Optional.empty())));
	}
	
	@Test
	public void testGetEntities_customRange() {
		assertEquals(toIndexEntityList(entity2, entity3, entity4, entity5, entity6), 
				toList(indexMocker.getIndex().getEntities(new Range(2147483655L, 2147483675L), new LinkedList<>(), new LinkedList<>(), Optional.empty())));
	}
	
	@Test
	public void testGetEntities_customSegmentRangesToInclude() {
		Optional<IncludedSegmentRangeInfo> includedSegmentRangeInfo = Optional.of(new IncludedSegmentRangeInfo());
		includedSegmentRangeInfo.get().addIncludedSegmentRangeInfo(new Range(2147483650L, 2147483659L), new Range(2147483650L, 2147483659L));
		includedSegmentRangeInfo.get().addIncludedSegmentRangeInfo(new Range(2147483670L, 2147483679L), new Range(2147483670L, 2147483679L));
		
		assertEquals(toIndexEntityList(entity1, entity2, entity5, entity6, entity7), 
				toList(indexMocker.getIndex().getEntities(maxRange, new LinkedList<>(), new LinkedList<>(), includedSegmentRangeInfo)));
	}
	
	@Test
	public void testGetEntities_customIndexKeyConditions() {
		@SuppressWarnings("unchecked")
		List<Condition<BlueKey>> indexKeyConditions = Arrays.asList(
				(key -> {
					if(key instanceof IndexCompositeKey) {
						key = ((IndexCompositeKey<BlueKey>)key).getIndexKey();
					}
					return key instanceof IntegerKey && ((IntegerKey)key).getId() > 15;	
				}),
				(key -> {
					if(key instanceof IndexCompositeKey) {
						key = ((IndexCompositeKey<BlueKey>)key).getIndexKey();
					}
					return key instanceof IntegerKey && ((IntegerKey)key).getId() <= 29;	
				}));
		assertEquals(toIndexEntityList(entity4, entity5, entity6, entity7), 
				toList(indexMocker.getIndex().getEntities(maxRange, indexKeyConditions, new LinkedList<>(), Optional.empty())));
	}
	
	@Test
	public void testGetEntities_customValueKeyConditions() {
		List<Condition<BlueKey>> valueKeyConditions = Arrays.asList(
				(key -> key instanceof TimeKey && ((TimeKey)key).getTime() > 15),
				(key -> key instanceof TimeKey && ((TimeKey)key).getTime() <= 29));
		assertEquals(toIndexEntityList(entity4, entity5, entity6), 
				toList(indexMocker.getIndex().getEntities(maxRange, new LinkedList<>(), valueKeyConditions, Optional.empty())));
	}

	private IntegerKey toIndexKey(BlueEntity<TestValue> collectionEntity) {
		return new IntegerKey(collectionEntity.getValue().getCupcakes());
	}

	private BlueEntity<BlueKey> toIndexEntity(BlueEntity<TestValue> collectionEntity) {
		return new BlueEntity<BlueKey>(new IndexCompositeKey<BlueKey>(toIndexKey(collectionEntity), collectionEntity.getKey()), collectionEntity.getKey());
	}

	private <T extends Serializable> List<BlueEntity<T>> toList(CloseableIterator<BlueEntity<T>> entities) {
		List<BlueEntity<T>> list = new LinkedList<>();
		while(entities.hasNext()) {
			list.add(entities.next());
		}
		return list;
	}

	@SafeVarargs
	private final List<BlueEntity<BlueKey>> toIndexEntityList(BlueEntity<TestValue>...entities) {
		return StreamUtils.stream(entities)
			.map(this::toIndexEntity)
			.collect(Collectors.toCollection(LinkedList::new));
	}

}
