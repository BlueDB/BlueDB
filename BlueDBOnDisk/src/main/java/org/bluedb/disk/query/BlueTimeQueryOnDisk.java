package org.bluedb.disk.query;

import java.io.Serializable;
import java.util.LinkedList;

import org.bluedb.api.BlueTimeQuery;
import org.bluedb.api.Condition;
import org.bluedb.disk.collection.ReadWriteBlueCollectionOnDisk;
import org.bluedb.disk.collection.ReadWriteBlueTimeCollectionOnDisk;

public class BlueTimeQueryOnDisk<T extends Serializable> extends BlueQueryOnDisk<T> implements BlueTimeQuery<T> {

	public BlueTimeQueryOnDisk(ReadWriteBlueCollectionOnDisk<T> collection) {
		super(collection);
	}

	@Override
	public BlueTimeQuery<T> where(Condition<T> c) {
		super.where(c);
		return this;
	}

	@Override
	public BlueTimeQueryOnDisk<T> afterTime(long time) {
		super.afterTime(time);
		return this;
	}

	@Override
	public BlueTimeQueryOnDisk<T> afterOrAtTime(long time) {
		super.afterOrAtTime(time);
		return this;
	}

	@Override
	public BlueTimeQueryOnDisk<T> beforeTime(long time) {
		super.beforeTime(time);
		return this;
	}

	@Override
	public BlueTimeQueryOnDisk<T> beforeOrAtTime(long time) {
		super.beforeOrAtTime(time);
		return this;
	}

	@Override
	public BlueTimeQueryOnDisk<T> byStartTime() {
		super.byStartTime();
		return this;
	}

	public BlueTimeQueryOnDisk<T> clone() {
		BlueTimeQueryOnDisk<T> clone = new BlueTimeQueryOnDisk<T>((ReadWriteBlueTimeCollectionOnDisk<T>)collection);
		clone.objectConditions = new LinkedList<>(objectConditions);
		clone.min = min;
		clone.max = max;
		clone.byStartTime = byStartTime;
		return clone;
	}
	
}
