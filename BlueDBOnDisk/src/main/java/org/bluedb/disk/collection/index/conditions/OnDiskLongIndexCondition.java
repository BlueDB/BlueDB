package org.bluedb.disk.collection.index.conditions;

import java.io.Serializable;

import org.bluedb.api.Condition;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.index.conditions.LongIndexCondition;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.LongTimeKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.collection.index.ReadableIndexOnDisk;

public class OnDiskLongIndexCondition<T extends Serializable> extends OnDiskIndexConditionBase<Long, T> implements LongIndexCondition {
	
	private long min = Long.MIN_VALUE;
	private long max = Long.MAX_VALUE;
	
	public OnDiskLongIndexCondition(ReadableIndexOnDisk<? extends ValueKey, T> index) {
		super(index);
	}

	@Override
	public LongIndexCondition isEqualTo(Long value) {
		super.isEqualTo(value);
		return this;
	}
	
	@Override
	public LongIndexCondition isInRange(long minValue, long maxValue) {
		if(hasIsEqualToBeenCalledAlready()) {
			throw new IllegalStateException("You cannot call LongIndexCondition#isInRange if you have already called LongIndexCondition#isEqualTo");
		}
		
		min = Math.max(min, minValue);
		max = Math.min(max, maxValue);
		updateRange(min, max);
		meets(indexedInt -> indexedInt >= minValue && indexedInt <= maxValue);
		return this;
	}

	@Override
	public LongIndexCondition isLessThan(long value) {
		if(hasIsEqualToBeenCalledAlready()) {
			throw new IllegalStateException("You cannot call LongIndexCondition#isLessThan if you have already called LongIndexCondition#isEqualTo");
		}
		
		max = Math.min(max, Math.min(value - 1, value)); // last part to avoid overflow errors
		updateRange(min, max);
		meets(indexedLong -> indexedLong < value);
		return this;
	}

	@Override
	public LongIndexCondition isLessThanOrEqualTo(long value) {
		if(hasIsEqualToBeenCalledAlready()) {
			throw new IllegalStateException("You cannot call LongIndexCondition#isLessThanOrEqualTo if you have already called LongIndexCondition#isEqualTo");
		}
		
		max = Math.min(max, value);
		updateRange(min, max);
		meets(indexedLong -> indexedLong <= value);
		return this;
	}

	@Override
	public LongIndexCondition isGreaterThan(long value) {
		if(hasIsEqualToBeenCalledAlready()) {
			throw new IllegalStateException("You cannot call LongIndexCondition#isGreaterThan if you have already called LongIndexCondition#isEqualTo");
		}
		
		min = Math.max(min, Math.max(value + 1, value)); // last part to avoid overflow errors
		updateRange(min, max);
		meets(indexedLong -> indexedLong > value);
		return this;
	}

	@Override
	public LongIndexCondition isGreaterThanOrEqualTo(long value) {
		if(hasIsEqualToBeenCalledAlready()) {
			throw new IllegalStateException("You cannot call LongIndexCondition#isGreaterThan if you have already called LongIndexCondition#isEqualTo");
		}
		
		min = Math.max(min, value);
		updateRange(min, max);
		meets(indexedLong -> indexedLong >= value);
		return this;
	}
	
	@Override
	public LongIndexCondition isIn(BlueSimpleSet<Long> values) {
		super.isIn(values);
		return this;
	}

	@Override
	public LongIndexCondition meets(Condition<Long> condition) {
		super.meets(condition);
		return this;
	}
	
	@Override
	protected ValueKey createKeyForIndexValue(Long value) {
		if(LongTimeKey.class.isAssignableFrom(getIndexKeyType())) {
			return new LongTimeKey(value);
		}
		
		return new LongKey(value);
	}
	
	@Override
	protected Long extractIndexValueFromKey(BlueKey indexKey) {
		if(indexKey instanceof LongTimeKey) {
			return ((LongTimeKey)indexKey).getId();
		}
		
		if(indexKey instanceof LongKey) {
			return ((LongKey)indexKey).getId();
		}
		
		return null;
	}

}
