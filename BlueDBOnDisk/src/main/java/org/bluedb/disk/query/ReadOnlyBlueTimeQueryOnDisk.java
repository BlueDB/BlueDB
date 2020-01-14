package org.bluedb.disk.query;

import java.io.Serializable;

import org.bluedb.api.Condition;
import org.bluedb.api.ReadBlueTimeQuery;
import org.bluedb.disk.collection.ReadOnlyBlueTimeCollectionOnDisk;

public class ReadOnlyBlueTimeQueryOnDisk<T extends Serializable> extends ReadOnlyBlueQueryOnDisk<T> implements ReadBlueTimeQuery<T> {

	public ReadOnlyBlueTimeQueryOnDisk(ReadOnlyBlueTimeCollectionOnDisk<T> collection) {
		super(collection);
	}

	@Override
	public ReadBlueTimeQuery<T> where(Condition<T> c) {
		super.where(c);
		return this;
	}

	@Override
	public ReadBlueTimeQuery<T> afterTime(long time) {
		min = Math.max(min, Math.max(time + 1,time)); // last part to avoid overflow errors
		return this;
	}

	@Override
	public ReadBlueTimeQuery<T> afterOrAtTime(long time) {
		min = Math.max(min, time);
		return this;
	}

	@Override
	public ReadBlueTimeQuery<T> beforeTime(long time) {
		max = Math.min(max, Math.min(time - 1,time)); // last part to avoid overflow errors
		return this;
	}

	@Override
	public ReadBlueTimeQuery<T> beforeOrAtTime(long time) {
		max = Math.min(max, time);
		return this;
	}

	@Override
	public ReadBlueTimeQuery<T> byStartTime() {
		byStartTime = true;
		return this;
	}

}
