package org.bluedb.disk.collection.index;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;

import org.bluedb.TestCloseableIterator;
import org.bluedb.api.CloseableIterator;
import org.bluedb.api.Condition;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.collection.index.conditions.IncludedSegmentRangeInfo;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.serialization.BlueEntity;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

public class ReadableIndexOnDiskMocker<I extends ValueKey, T extends Serializable> {
	@Mock private ReadableIndexOnDisk<I, T> index;
	
	public ReadableIndexOnDiskMocker() {
		MockitoAnnotations.initMocks(this);
	}
	
	public ReadableIndexOnDisk<I, T> getIndex() {
		return index;
	}

	public void setIndexedCollectionType(Class<T> indexedCollectionType) {
		Mockito.doReturn(indexedCollectionType).when(index).getCollectionType();
	}
	
	public void setIndexKeyType(Class<I> indexKeyType) {
		Mockito.doReturn(indexKeyType).when(index).getType();
	}
	
	public void setIndexName(String indexName) {
		Mockito.doReturn(indexName).when(index).getName();
	}
	
	public void setIndexPath(Path indexPath) {
		Mockito.doReturn(indexPath).when(index).getIndexPath();
	}
	
	public void setIndexExtractor(TestIndexKeyExtractor<T> indexKeyExtractor) {
		@SuppressWarnings("unchecked")
		Answer<List<? extends ValueKey>> answer = invocation -> {
			return indexKeyExtractor.extractIndexKeysFromValue((BlueEntity<T>) invocation.getArguments()[0]);
		};
		Mockito.doAnswer(answer).when(index).extractIndexKeys(Mockito.any());
	}
	
	public void setIndexKeyToIndexSegmentRangeMapper(TestKeyToRangeMapper keyToRangeMapper) {
		Answer<Range> answer = invocation -> {
			return keyToRangeMapper.map((BlueKey) invocation.getArguments()[0]);
		};
		Mockito.doAnswer(answer).when(index).getIndexSegmentRangeForIndexKey(Mockito.any());
	}
	
	public void setValueKeyToCollectionSegmentRangeMapper(TestKeyToRangeMapper keyToRangeMapper) {
		Answer<Range> answer = invocation -> {
			return keyToRangeMapper.map((BlueKey) invocation.getArguments()[0]);
		};
		Mockito.doAnswer(answer).when(index).getCollectionSegmentRangeForValueKey(Mockito.any());
	}
	
	/** Must be called after setIndexExtractor*/
	public void setEntities(List<BlueEntity<T>> collectionEntities) {
		TreeMap<IndexCompositeKey<BlueKey>, BlueKey> indexData = new TreeMap<>();
		for(BlueEntity<T> entity : collectionEntities) {
			List<? extends ValueKey> indexKeysForEntity = index.extractIndexKeys(entity);
			if(indexKeysForEntity != null) {
				for(ValueKey indexKeyForEntity : indexKeysForEntity) {
					indexData.put(new IndexCompositeKey<BlueKey>(indexKeyForEntity, entity.getKey()), entity.getKey());
				}
			}
		}
		
		@SuppressWarnings("unchecked")
		Answer<CloseableIterator<BlueEntity<BlueKey>>> answer = invocation -> {
			Range range = (Range) invocation.getArguments()[0];
			List<Condition<BlueKey>> indexKeyConditions = (List<Condition<BlueKey>>) invocation.getArguments()[1];
			List<Condition<BlueKey>> objectConditions = (List<Condition<BlueKey>>) invocation.getArguments()[2];
			Optional<IncludedSegmentRangeInfo> segmentRangesToInclude = (Optional<IncludedSegmentRangeInfo>) invocation.getArguments()[3];
			
			List<BlueEntity<BlueKey>> matchingEntities = new LinkedList<>();
			
			for(Entry<IndexCompositeKey<BlueKey>, BlueKey> entry : indexData.entrySet()) {
				long groupingNumber = entry.getKey().getGroupingNumber();
				if(range.containsInclusive(groupingNumber) &&
						(!segmentRangesToInclude.isPresent() || segmentRangesToInclude.get().getSegmentRangeGroupingNumberRangePairs().stream().anyMatch(rangeEntry -> rangeEntry.getKey().containsInclusive(groupingNumber) && rangeEntry.getValue().containsInclusive(groupingNumber))) &&
						indexKeyConditions.stream().allMatch(condition -> condition.test(entry.getKey())) &&
						objectConditions.stream().allMatch(condition -> condition.test(entry.getValue()))) {
					matchingEntities.add(new BlueEntity<BlueKey>(entry.getKey(), entry.getValue()));
				}
			}
			
			return new TestCloseableIterator<>(matchingEntities.iterator());
		};
		
		Mockito.doAnswer(answer).when(index).getEntities(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
	}
	
	/** Must be called after setIndexExtractor*/
	public void setEntitiesToReturnNonCompositeIndexKeys(List<BlueEntity<T>> collectionEntities) {
		TreeMap<IndexCompositeKey<BlueKey>, BlueKey> indexData = new TreeMap<>();
		for(BlueEntity<T> entity : collectionEntities) {
			List<? extends ValueKey> indexKeysForEntity = index.extractIndexKeys(entity);
			if(indexKeysForEntity != null) {
				for(ValueKey indexKeyForEntity : indexKeysForEntity) {
					indexData.put(new IndexCompositeKey<BlueKey>(indexKeyForEntity, entity.getKey()), entity.getKey());
				}
			}
		}
		
		@SuppressWarnings("unchecked")
		Answer<CloseableIterator<BlueEntity<BlueKey>>> answer = invocation -> {
			Range range = (Range) invocation.getArguments()[0];
			List<Condition<BlueKey>> indexKeyConditions = (List<Condition<BlueKey>>) invocation.getArguments()[1];
			List<Condition<BlueKey>> objectConditions = (List<Condition<BlueKey>>) invocation.getArguments()[2];
			Optional<IncludedSegmentRangeInfo> segmentRangesToInclude = (Optional<IncludedSegmentRangeInfo>) invocation.getArguments()[3];
			
			List<BlueEntity<BlueKey>> matchingEntities = new LinkedList<>();
			
			for(Entry<IndexCompositeKey<BlueKey>, BlueKey> entry : indexData.entrySet()) {
				long groupingNumber = entry.getKey().getGroupingNumber();
				if(range.containsInclusive(groupingNumber) &&
						(!segmentRangesToInclude.isPresent() || segmentRangesToInclude.get().getSegmentRangeGroupingNumberRangePairs().stream().anyMatch(rangeEntry -> rangeEntry.getKey().containsInclusive(groupingNumber) && rangeEntry.getValue().containsInclusive(groupingNumber))) &&
						indexKeyConditions.stream().allMatch(condition -> condition.test(entry.getKey())) &&
						objectConditions.stream().allMatch(condition -> condition.test(entry.getValue()))) {
					matchingEntities.add(new BlueEntity<BlueKey>(entry.getKey().getValueKey(), entry.getValue()));
				}
			}
			
			return new TestCloseableIterator<>(matchingEntities.iterator());
		};
		
		Mockito.doAnswer(answer).when(index).getEntities(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
	}
	
	@FunctionalInterface
	public static interface TestIndexKeyExtractor<T extends Serializable> {
		public List<? extends ValueKey> extractIndexKeysFromValue(BlueEntity<T> value);
	}
	
	@FunctionalInterface
	public static interface TestKeyToRangeMapper {
		public Range map(BlueKey key);
	}
}
