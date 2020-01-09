package org.bluedb.disk.query;

import java.io.Serializable;
import java.util.LinkedList;

import org.bluedb.api.BlueQuery;
import org.bluedb.api.Condition;
import org.bluedb.api.Mapper;
import org.bluedb.api.Updater;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.collection.BlueCollectionOnDisk;
import org.bluedb.disk.collection.task.DeleteMultipleTask;
import org.bluedb.disk.collection.task.ReplaceMultipleTask;
import org.bluedb.disk.collection.task.UpdateMultipleTask;

public class BlueQueryOnDisk<T extends Serializable> extends ReadOnlyBlueQueryOnDisk<T> implements BlueQuery<T> {

	public BlueQueryOnDisk(BlueCollectionOnDisk<T> collection) {
		super(collection);
	}

	@Override
	public BlueQuery<T> where(Condition<T> c) {
		super.where(c);
		return this;
	}

	//TODO: Remember that this is duplicated so you might want to pull of some strategy pattern shiz to share code here
	@Override
	public void delete() throws BlueDbException {
		Runnable deleteAllTask = new DeleteMultipleTask<T>(collection, clone());
		collection.executeTask(deleteAllTask);
	}

	@Override
	public void update(Updater<T> updater) throws BlueDbException {
		Runnable updateMultipleTask = new UpdateMultipleTask<T>(collection, clone(), updater);
		collection.executeTask(updateMultipleTask);
	}

	@Override
	public void replace(Mapper<T> mapper) throws BlueDbException {
		Runnable updateMultipleTask = new ReplaceMultipleTask<T>(collection, clone(), mapper);
		collection.executeTask(updateMultipleTask);
	}

	public BlueQueryOnDisk<T> clone() {
		BlueQueryOnDisk<T> clone = new BlueQueryOnDisk<T>((BlueCollectionOnDisk<T>)collection);
		clone.objectConditions = new LinkedList<>(objectConditions);
		clone.min = min;
		clone.max = max;
		clone.byStartTime = byStartTime;
		return clone;
	}
}
