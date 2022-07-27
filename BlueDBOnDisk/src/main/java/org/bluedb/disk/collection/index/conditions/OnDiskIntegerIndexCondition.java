package org.bluedb.disk.collection.index.conditions;

import java.io.Serializable;

import org.bluedb.api.Condition;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.index.conditions.IntegerIndexCondition;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.collection.index.ReadableIndexOnDisk;

public class OnDiskIntegerIndexCondition<T extends Serializable> extends OnDiskIndexConditionBase<Integer, T> implements IntegerIndexCondition {
	
	private int min = Integer.MIN_VALUE;
	private int max = Integer.MAX_VALUE;
	
	public OnDiskIntegerIndexCondition(ReadableIndexOnDisk<? extends ValueKey, T> index) {
		super(index);
	}

	@Override
	public IntegerIndexCondition isEqualTo(Integer value) {
		super.isEqualTo(value);
		return this;
	}
	
	@Override
	public IntegerIndexCondition isInRange(int minValue, int maxValue) {
		if(hasIsEqualToBeenCalledAlready()) {
			throw new IllegalStateException("You cannot call IntegerIndexCondition#isInRange if you have already called IntegerIndexCondition#isEqualTo");
		}
		
		min = Math.max(min, minValue);
		max = Math.min(max, maxValue);
		updateRange(min, max);
		meets(indexedInt -> indexedInt >= minValue && indexedInt <= maxValue);
		return this;
	}

	@Override
	public IntegerIndexCondition isLessThan(int value) {
		if(hasIsEqualToBeenCalledAlready()) {
			throw new IllegalStateException("You cannot call IntegerIndexCondition#isLessThan if you have already called IntegerIndexCondition#isEqualTo");
		}
		
		max = Math.min(max, Math.min(value - 1, value)); // last part to avoid overflow errors
		updateRange(min, max);
		meets(indexedInt -> indexedInt < value);
		return this;
	}

	@Override
	public IntegerIndexCondition isLessThanOrEqualTo(int value) {
		if(hasIsEqualToBeenCalledAlready()) {
			throw new IllegalStateException("You cannot call IntegerIndexCondition#isLessThanOrEqualTo if you have already called IntegerIndexCondition#isEqualTo");
		}
		
		max = Math.min(max, value);
		updateRange(min, max);
		meets(indexedInt -> indexedInt <= value);
		return this;
	}

	@Override
	public IntegerIndexCondition isGreaterThan(int value) {
		if(hasIsEqualToBeenCalledAlready()) {
			throw new IllegalStateException("You cannot call IntegerIndexCondition#isGreaterThan if you have already called IntegerIndexCondition#isEqualTo");
		}
		
		min = Math.max(min, Math.max(value + 1, value)); // last part to avoid overflow errors
		updateRange(min, max);
		meets(indexedInt -> indexedInt > value);
		return this;
	}

	@Override
	public IntegerIndexCondition isGreaterThanOrEqualTo(int value) {
		if(hasIsEqualToBeenCalledAlready()) {
			throw new IllegalStateException("You cannot call IntegerIndexCondition#isGreaterThan if you have already called IntegerIndexCondition#isEqualTo");
		}
		
		min = Math.max(min, value);
		updateRange(min, max);
		meets(indexedInt -> indexedInt >= value);
		return this;
	}
	
	@Override
	public IntegerIndexCondition isIn(BlueSimpleSet<Integer> values) {
		super.isIn(values);
		return this;
	}
	
	@Override
	public IntegerIndexCondition meets(Condition<Integer> condition) {
		super.meets(condition);
		return this;
	}
	
	@Override
	protected ValueKey createKeyForIndexValue(Integer value) {
		return new IntegerKey(value);
	}
	
	@Override
	protected Integer extractIndexValueFromKey(BlueKey indexKey) {
		if(indexKey instanceof IntegerKey) {
			return ((IntegerKey)indexKey).getId();
		}
		return null;
	}

}
