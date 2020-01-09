package org.bluedb.disk.query;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.bluedb.api.CloseableIterator;
import org.bluedb.api.Condition;
import org.bluedb.api.ReadOnlyBlueQuery;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.collection.ReadOnlyBlueCollectionOnDisk;
import org.bluedb.disk.collection.CollectionValueIterator;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.serialization.BlueEntity;

public class ReadOnlyBlueQueryOnDisk<T extends Serializable> implements ReadOnlyBlueQuery<T> {

	protected ReadOnlyBlueCollectionOnDisk<T> collection;
	protected List<Condition<T>> objectConditions = new LinkedList<>();
	protected long max = Long.MAX_VALUE;
	protected long min = Long.MIN_VALUE;
	protected boolean byStartTime = false;

	public ReadOnlyBlueQueryOnDisk(ReadOnlyBlueCollectionOnDisk<T> collection) {
		this.collection = collection;
	}

	@Override
	public ReadOnlyBlueQuery<T> where(Condition<T> c) {
		if (c != null) {
			objectConditions.add(c);
		}
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

	@Override
	public String toString() {
		return "<" + this.getClass().getSimpleName() + " [" + min + ", " + max + "] with " + objectConditions.size() + " conditions>";
	}

	public Range getRange() {
		return new Range(min, max);
	}
}
