package org.bluedb.disk.query;

import java.io.Serializable;
import java.util.Set;

import org.bluedb.api.Condition;
import org.bluedb.api.ReadBlueTimeQuery;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.index.conditions.BlueIndexCondition;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.collection.ReadOnlyCollectionOnDisk;

public class ReadOnlyTimeQueryOnDisk<T extends Serializable> extends ReadOnlyQueryOnDisk<T> implements ReadBlueTimeQuery<T> {

	public ReadOnlyTimeQueryOnDisk(ReadOnlyCollectionOnDisk<T> collection) {
		super(collection);
	}

	@Override
	public ReadBlueTimeQuery<T> where(Condition<T> c) {
		super.where(c);
		return this;
	}
	
	@Override
	public ReadBlueTimeQuery<T> where(BlueIndexCondition<?> indexCondition) {
		super.where(indexCondition);
		return this;
	}
	
	@Override
	public ReadBlueTimeQuery<T> whereKeyIsIn(Set<BlueKey> keys) {
		super.whereKeyIsIn(keys);
		return this;
	}
	
	@Override
	public ReadBlueTimeQuery<T> whereKeyIsIn(BlueSimpleSet<BlueKey> keys) {
		super.whereKeyIsIn(keys);
		return this;
	}

	@Override
	public ReadBlueTimeQuery<T> whereKeyIsActive() {
		super.whereKeyIsActive();
		return this;
	}

	@Override
	public ReadBlueTimeQuery<T> whereKeyIsNotActive() {
		super.whereKeyIsNotActive();
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
