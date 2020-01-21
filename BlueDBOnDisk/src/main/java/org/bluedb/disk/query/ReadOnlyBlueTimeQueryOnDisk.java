package org.bluedb.disk.query;

import java.io.Serializable;

import org.bluedb.api.Condition;
import org.bluedb.api.ReadBlueTimeQuery;
import org.bluedb.disk.collection.ReadOnlyBlueCollectionOnDisk;

public class ReadOnlyBlueTimeQueryOnDisk<T extends Serializable> extends ReadOnlyBlueQueryOnDisk<T> implements ReadBlueTimeQuery<T> {

	public ReadOnlyBlueTimeQueryOnDisk(ReadOnlyBlueCollectionOnDisk<T> collection) {
		super(collection);
	}

	@Override
	public ReadBlueTimeQuery<T> where(Condition<T> c) {
		super.where(c);
		return this;
	}

	@Override
	public ReadBlueTimeQuery<T> afterTime(long time) {
		super.afterTime(time);
		return this;
	}

	@Override
	public ReadBlueTimeQuery<T> afterOrAtTime(long time) {
		super.afterOrAtTime(time);
		return this;
	}

	@Override
	public ReadBlueTimeQuery<T> beforeTime(long time) {
		super.beforeTime(time);
		return this;
	}

	@Override
	public ReadBlueTimeQuery<T> beforeOrAtTime(long time) {
		super.beforeOrAtTime(time);
		return this;
	}

	@Override
	public ReadBlueTimeQuery<T> byStartTime() {
		super.byStartTime();
		return this;
	}

}
