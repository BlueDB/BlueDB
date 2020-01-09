package org.bluedb.disk.query;

import java.io.Serializable;

import org.bluedb.api.Condition;
import org.bluedb.api.ReadOnlyBlueTimeQuery;
import org.bluedb.disk.collection.ReadOnlyBlueTimeCollectionOnDisk;

public class ReadOnlyBlueTimeQueryOnDisk<T extends Serializable> extends ReadOnlyBlueQueryOnDisk<T> implements ReadOnlyBlueTimeQuery<T> {

	public ReadOnlyBlueTimeQueryOnDisk(ReadOnlyBlueTimeCollectionOnDisk<T> collection) {
		super(collection);
	}

	@Override
	public ReadOnlyBlueTimeQuery<T> where(Condition<T> c) {
		super.where(c);
		return this;
	}

	@Override
	public ReadOnlyBlueTimeQuery<T> afterTime(long time) {
		min = Math.max(min, Math.max(time + 1,time)); // last part to avoid overflow errors
		return this;
	}

	@Override
	public ReadOnlyBlueTimeQuery<T> afterOrAtTime(long time) {
		min = Math.max(min, time);
		return this;
	}

	@Override
	public ReadOnlyBlueTimeQuery<T> beforeTime(long time) {
		max = Math.min(max, Math.min(time - 1,time)); // last part to avoid overflow errors
		return this;
	}

	@Override
	public ReadOnlyBlueTimeQuery<T> beforeOrAtTime(long time) {
		max = Math.min(max, time);
		return this;
	}

	@Override
	public ReadOnlyBlueTimeQuery<T> byStartTime() {
		byStartTime = true;
		return this;
	}

}
