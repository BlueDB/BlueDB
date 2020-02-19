package org.bluedb.disk.query;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.bluedb.api.CloseableIterator;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.serialization.BlueEntity;

public class DummyQuery<T extends Serializable> extends ReadOnlyTimeQueryOnDisk<T> {

	public DummyQuery() {
		super(null);
	}

	@Override
	public CloseableIterator<T> getIterator() throws BlueDbException {
		return new CloseableIterator<T>() {

			@Override
			public void close() throws IOException {
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
