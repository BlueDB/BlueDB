package org.bluedb.disk.query;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Set;

import org.bluedb.api.BlueQuery;
import org.bluedb.api.Condition;
import org.bluedb.api.Mapper;
import org.bluedb.api.Updater;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.collection.task.BatchQueryChangeTask;
import org.bluedb.disk.recovery.EntityToChangeMapper;
import org.bluedb.disk.recovery.IndividualChange;

public class QueryOnDisk<T extends Serializable> extends ReadOnlyQueryOnDisk<T> implements BlueQuery<T> {

	ReadWriteCollectionOnDisk<T> writeableCollection;

	public QueryOnDisk(ReadWriteCollectionOnDisk<T> collection) {
		super(collection);
		writeableCollection = collection;
	}

	@Override
	public BlueQuery<T> where(Condition<T> c) {
		super.where(c);
		return this;
	}
	
	@Override
	public BlueQuery<T> whereKeyIsIn(Set<BlueKey> keys) {
		super.whereKeyIsIn(keys);
		return this;
	}
	
	@Override
	public BlueQuery<T> whereKeyIsIn(BlueSimpleSet<BlueKey> keys) {
		super.whereKeyIsIn(keys);
		return this;
	}

	@Override
	public void delete() throws BlueDbException {
		Runnable deleteAllTask = new DeleteMultipleTask<T>(writeableCollection, clone());
		writeableCollection.executeTask(deleteAllTask);
	}

	@Override
	public void update(Updater<T> updater) throws BlueDbException {
		Runnable updateMultipleTask = new UpdateMultipleTask<T>(writeableCollection, clone(), updater);
		writeableCollection.executeTask(updateMultipleTask);
	}

	@Override
	public void replace(Mapper<T> mapper) throws BlueDbException {
		Runnable updateMultipleTask = new ReplaceMultipleTask<T>(writeableCollection, clone(), mapper);
		writeableCollection.executeTask(updateMultipleTask);
	}

	public QueryOnDisk<T> clone() {
		QueryOnDisk<T> clone = new QueryOnDisk<T>((ReadWriteCollectionOnDisk<T>)collection);
		clone.objectConditions = new LinkedList<>(objectConditions);
		clone.keyConditions = new LinkedList<>(keyConditions);
		clone.keySetsToInclude = new LinkedList<>(keySetsToInclude);
		clone.min = min;
		clone.max = max;
		clone.byStartTime = byStartTime;
		return clone;
		
	}
}
