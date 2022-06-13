package org.bluedb.disk.query;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Set;

import org.bluedb.api.BlueTimeQuery;
import org.bluedb.api.Condition;
import org.bluedb.api.TimeEntityMapper;
import org.bluedb.api.TimeEntityUpdater;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.conditions.BlueIndexCondition;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.collection.ReadWriteTimeCollectionOnDisk;
import org.bluedb.disk.collection.task.BatchQueryChangeTask;
import org.bluedb.disk.recovery.EntityToChangeMapper;
import org.bluedb.disk.recovery.IndividualChange;

public class TimeQueryOnDisk<T extends Serializable> extends QueryOnDisk<T> implements BlueTimeQuery<T> {

	public TimeQueryOnDisk(ReadWriteCollectionOnDisk<T> collection) {
		super(collection);
	}

	@Override
	public BlueTimeQuery<T> where(Condition<T> c) {
		super.where(c);
		return this;
	}
	
	@Override
	public BlueTimeQuery<T> where(BlueIndexCondition<?> indexCondition) {
		super.where(indexCondition);
		return this;
	}
	
	@Override
	public BlueTimeQuery<T> whereKeyIsIn(Set<BlueKey> keys) {
		super.whereKeyIsIn(keys);
		return this;
	}
	
	@Override
	public BlueTimeQuery<T> whereKeyIsIn(BlueSimpleSet<BlueKey> keys) {
		super.whereKeyIsIn(keys);
		return this;
	}

	@Override
	public BlueTimeQuery<T> whereKeyIsActive() {
		super.whereKeyIsActive();
		return this;
	}

	@Override
	public BlueTimeQuery<T> whereKeyIsNotActive() {
		super.whereKeyIsNotActive();
		return this;
	}
	
	@Override
	public BlueTimeQuery<T> afterTime(long time) {
		super.afterTime(time);
		return this;
	}

	@Override
	public BlueTimeQuery<T> afterOrAtTime(long time) {
		super.afterOrAtTime(time);
		return this;
	}

	@Override
	public BlueTimeQuery<T> beforeTime(long time) {
		super.beforeTime(time);
		return this;
	}

	@Override
	public BlueTimeQuery<T> beforeOrAtTime(long time) {
		super.beforeOrAtTime(time);
		return this;
	}

	@Override
	public BlueTimeQuery<T> byStartTime() {
		super.byStartTime();
		return this;
	}

	public TimeQueryOnDisk<T> clone() {
		TimeQueryOnDisk<T> clone = new TimeQueryOnDisk<T>((ReadWriteTimeCollectionOnDisk<T>)collection);
		clone.objectConditions = new LinkedList<>(objectConditions);
		clone.keyConditions = new LinkedList<>(keyConditions);
		clone.keySetsToInclude = new LinkedList<>(keySetsToInclude);
		clone.min = min;
		clone.max = max;
		clone.byStartTime = byStartTime;
		return clone;
	}

	@Override
	public BlueTimeQuery<T> updateKeyAndValue(TimeEntityUpdater<T> updater) throws BlueDbException {
		EntityToChangeMapper<T> changeMapper = entity -> {
			return IndividualChange.createUpdateKeyAndValueChange(entity.getKey(), entity.getValue(), updater, writeableCollection.getSerializer());
		};
		
		String description = "Update using query " + this;
		Runnable task = new BatchQueryChangeTask<T>(description, writeableCollection, clone(), changeMapper);
		writeableCollection.executeTask(task);
		return this;
	}

	@Override
	public BlueTimeQuery<T> replaceKeyAndValue(TimeEntityMapper<T> mapper) throws BlueDbException {
		EntityToChangeMapper<T> changeMapper = entity -> {
			return IndividualChange.createReplaceKeyAndValueChange(entity.getKey(), entity.getValue(), mapper, writeableCollection.getSerializer());
		};
		
		String description = "Replace using query " + this;
		Runnable task = new BatchQueryChangeTask<T>(description, writeableCollection, clone(), changeMapper);
		writeableCollection.executeTask(task);
		return this;
	}
	
}
