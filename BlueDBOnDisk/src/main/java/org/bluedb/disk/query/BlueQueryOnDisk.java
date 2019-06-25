package org.bluedb.disk.query;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import org.bluedb.api.BlueQuery;
import org.bluedb.api.CloseableIterator;
import org.bluedb.api.Condition;
import org.bluedb.api.Mapper;
import org.bluedb.api.Updater;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.collection.BlueCollectionOnDisk;
import org.bluedb.disk.collection.CollectionValueIterator;
import org.bluedb.disk.collection.task.DeleteMultipleTask;
import org.bluedb.disk.collection.task.ReplaceMultipleTask;
import org.bluedb.disk.collection.task.UpdateMultipleTask;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.serialization.BlueEntity;

public class BlueQueryOnDisk<T extends Serializable> implements BlueQuery<T> {

	private BlueCollectionOnDisk<T> collection;
	private List<Condition<T>> objectConditions = new LinkedList<>();
	private long max = Long.MAX_VALUE;
	private long min = Long.MIN_VALUE;
	private boolean byStartTime = false;

	public BlueQueryOnDisk(BlueCollectionOnDisk<T> collection) {
		this.collection = collection;
	}

	@Override
	public BlueQuery<T> where(Condition<T> c) {
		if (c != null) {
			objectConditions.add(c);
		}
		return this;
	}

	@Override
	public BlueQuery<T> afterTime(long time) {
		min = Math.max(min, Math.max(time + 1,time)); // last part to avoid overflow errors
		return this;
	}

	@Override
	public BlueQuery<T> afterOrAtTime(long time) {
		min = Math.max(min, time);
		return this;
	}

	@Override
	public BlueQuery<T> beforeTime(long time) {
		max = Math.min(max, Math.min(time - 1,time)); // last part to avoid overflow errors
		return this;
	}

	@Override
	public BlueQuery<T> beforeOrAtTime(long time) {
		max = Math.min(max, time);
		return this;
	}

	@Override
	public BlueQuery<T> byStartTime() {
		byStartTime = true;
		return this;
	}

	@Override
	public List<T> getList() throws BlueDbException {
		return Blutils.map(getEntities(), (e) -> e.getValue());
	}

	@Override
	public CloseableIterator<T> getIterator() throws BlueDbException {
		Range range = new Range(min, max);
		return new CollectionValueIterator<T>(collection.getSegmentManager(), range, byStartTime, objectConditions);
	}

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

	@Override
	public int count() throws BlueDbException {
		CloseableIterator<T> iter = getIterator();
		int count = 0;
		while (iter.hasNext()) {
			count++;
			iter.next();
		}
		return count;
	}

	public List<BlueEntity<T>> getEntities() throws BlueDbException {
		return collection.findMatches(getRange(), objectConditions, byStartTime);
	}

	public BlueQueryOnDisk<T> clone() {
		BlueQueryOnDisk<T> clone = new BlueQueryOnDisk<T>(collection);
		clone.objectConditions = new LinkedList<>(objectConditions);
		clone.min = min;
		clone.max = max;
		clone.byStartTime = byStartTime;
		return clone;
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getSimpleName() + " [" + min + ", " + max + "] with " + objectConditions.size() + " conditions>";
	}

	public Range getRange() {
		return new Range(min, max);
	}
}
