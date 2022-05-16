package org.bluedb.disk.collection.index.conditions;

import java.io.Serializable;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.bluedb.api.CloseableIterator;
import org.bluedb.api.Condition;
import org.bluedb.api.datastructures.BlueSimpleIterator;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.index.conditions.BlueIndexCondition;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.StreamUtils;
import org.bluedb.disk.collection.index.IndexCompositeKey;
import org.bluedb.disk.collection.index.ReadableIndexOnDisk;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.serialization.BlueEntity;

public abstract class OnDiskIndexConditionBase<I extends Serializable, T extends Serializable> implements OnDiskIndexCondition<I, T> {
	
	private final ReadableIndexOnDisk<? extends ValueKey, T> index;
	
	private final List<BlueSimpleSet<I>> validValueSets = new LinkedList<>();
	private final List<Condition<I>> conditions = new LinkedList<>();
	
	private I equalToValue = null;
	private Range range = new Range(Long.MIN_VALUE, Long.MAX_VALUE);
	
	public OnDiskIndexConditionBase(ReadableIndexOnDisk<? extends ValueKey, T> index) {
		this.index = index;
	}
	
	@Override
	public Class<T> getIndexedCollectionType() {
		return index.getCollectionType();
	}
	
	@Override
	public Class<? extends ValueKey> getIndexKeyType() {
		return index.getType();
	}
	
	@Override
	public String getIndexName() {
		return index.getName();
	}
	
	@Override
	public Path getIndexPath() {
		return index.getIndexPath();
	}
	
	@Override
	public BlueIndexCondition<I> isEqualTo(I value) {
		if(value == null) {
			throw new InvalidParameterException("Null is an invalid parameter for BlueIndexCondition#isEqualTo");
		}
		
		this.equalToValue = value;
		updateRange(equalToValue, equalToValue);
		conditions.add(indexedValue -> Objects.equals(indexedValue, equalToValue));
		return this;
	}
	
	@Override
	public BlueIndexCondition<I> isIn(BlueSimpleSet<I> values) {
		if(values == null) {
			throw new InvalidParameterException("Null is an invalid parameter for BlueIndexCondition#isIn");
		}
		
		if(hasIsEqualToBeenCalledAlready()) {
			throw new IllegalStateException("You cannot call BlueIndexCondition#isIn if you have already called BlueIndexCondition#isEqualTo");
		}
		
		validValueSets.add(values);
		return this;
	}

	@Override
	public BlueIndexCondition<I> meets(Condition<I> condition) {
		if(condition == null) {
			throw new InvalidParameterException("Null is an invalid parameter for BlueIndexCondition#meets");
		}
		
		if(hasIsEqualToBeenCalledAlready()) {
			throw new IllegalStateException("You cannot call BlueIndexCondition#matches if you have already called BlueIndexCondition#isEqualTo");
		}
		
		conditions.add(condition);
		return this;
	}
	
	protected void updateRange(I start, I end) {
		range = new Range(createKeyForIndexValue(start).getGroupingNumber(), createKeyForIndexValue(end).getGroupingNumber());
	}
	
	protected boolean hasIsEqualToBeenCalledAlready() {
		return equalToValue != null;
	}

	@Override
	public IncludedSegmentRangeInfo getSegmentRangeInfoToIncludeInCollectionQuery() {
		IncludedSegmentRangeInfo includedCollectionSegmentRangeInfo = new IncludedSegmentRangeInfo();
		
		List<Condition<BlueKey>> indexKeyConditions = new LinkedList<>();
		indexKeyConditions.add(this::doesIndexKeyMatch);
		
		//We're looking at the index key, not the key of the original object
		List<Condition<BlueKey>> objectConditions = new LinkedList<>();
		
		Optional<IncludedSegmentRangeInfo> includedIndexSegmentRangeInfo = getIndexSegmentRangesToInclude();
		try (CloseableIterator<BlueEntity<BlueKey>> entityIterator = index.getEntities(range, indexKeyConditions, objectConditions, includedIndexSegmentRangeInfo)) {
			while (entityIterator.hasNext()) {
				BlueKey key = entityIterator.next().getKey();
				if(key instanceof IndexCompositeKey) {
					IndexCompositeKey<?> indexCompositeKey = (IndexCompositeKey<?>) key;
					BlueKey valueKey = indexCompositeKey.getValueKey();
					Range collectionSegmentRange = index.getCollectionSegmentRangeForValueKey(valueKey);
					includedCollectionSegmentRangeInfo.addIncludedSegmentRangeInfo(collectionSegmentRange, valueKey.getGroupingNumber());
				}
			}
		}
		return includedCollectionSegmentRangeInfo;
	}
	
	private boolean doesIndexKeyMatch(BlueKey key) {
		if(!range.containsInclusive(key.getGroupingNumber())) {
			return false;
		}
		
		if(key instanceof IndexCompositeKey) {
			key = ((IndexCompositeKey<?>) key).getIndexKey();
		}
		
		I indexValue = extractIndexValueFromKey(key);
		
		if(indexValue != null) {
			if(!Blutils.meetsConditions(conditions, indexValue)) {
				return false;
			}

			for(BlueSimpleSet<I> validValueSet : validValueSets) {
				if(!validValueSet.contains(indexValue)) {
					return false;
				}
			}
			
			return true;
		}
		
		return false;
	}
	
	protected abstract I extractIndexValueFromKey(BlueKey indexKey);

	private Optional<IncludedSegmentRangeInfo> getIndexSegmentRangesToInclude() {
		IncludedSegmentRangeInfo includedSegmentRangeInfo = new IncludedSegmentRangeInfo();
		
		for(BlueSimpleSet<I> validValueSet : validValueSets) {
			try(BlueSimpleIterator<I> validValueIterator = validValueSet.iterator()) {
				while(validValueIterator.hasNext()) {
					I validValue = validValueIterator.next();
					if(validValue != null) {
						ValueKey keyForIndexValue = createKeyForIndexValue(validValue);
						Range indexSegmentRange = index.getIndexSegmentRangeForIndexKey(keyForIndexValue);
						includedSegmentRangeInfo.addIncludedSegmentRangeInfo(indexSegmentRange, keyForIndexValue.getGroupingNumber());
					}
				}
			}
		}
		
		if(includedSegmentRangeInfo.isEmpty()) {
			return Optional.empty();
		} else {
			return Optional.of(includedSegmentRangeInfo);
		}
	}
	
	protected abstract ValueKey createKeyForIndexValue(I value);
	
	@Override
	public boolean test(BlueEntity<T> entity) {
		return StreamUtils.stream(extractIndexKeysFromEntity(entity))
			.anyMatch(this::doesIndexKeyMatch);
	}

	protected List<? extends ValueKey> extractIndexKeysFromEntity(BlueEntity<T> entity) {
		return index.extractIndexKeys(entity);
	}
}
