package org.bluedb.disk.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.bluedb.api.CloseableIterator;
import org.bluedb.api.Condition;
import org.bluedb.api.ReadBlueTimeQuery;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.conditions.BlueIndexCondition;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.serialization.BlueEntity;

public class DummyQuery<T extends Serializable> extends ReadOnlyTimeQueryOnDisk<T> {

	public DummyQuery() {
		super(null);
	}

	@Override
	public ReadBlueTimeQuery<T> where(Condition<T> c) {
		return this;
	}
	
	@Override
	public ReadBlueTimeQuery<T> where(BlueIndexCondition<?> indexCondition) {
		return this;
	}
	
	@Override
	public ReadBlueTimeQuery<T> whereKeyIsIn(Set<BlueKey> keys) {
		return this;
	}
	
	@Override
	public ReadBlueTimeQuery<T> whereKeyIsIn(BlueSimpleSet<BlueKey> keys) {
		return this;
	}

	@Override
	public CloseableIterator<T> getIterator() throws BlueDbException {
		return new CloseableIterator<T>() {

			@Override
			public void close() {
			}

			@Override
			public boolean hasNext() {
				return false;
			}

			@Override
			public T next() {
				return null;
			}

			@Override
			public T peek() {
				return null;
			}

			@Override
			public void keepAlive() {
				
			}
			
		};
	}

	@Override
	public List<BlueEntity<T>> getEntities() throws BlueDbException {
		return new ArrayList<>();
	}
}
