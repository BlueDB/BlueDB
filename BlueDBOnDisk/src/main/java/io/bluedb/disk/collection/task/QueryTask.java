package io.bluedb.disk.collection.task;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.exceptions.DuplicateKeyException;

public abstract class QueryTask implements Runnable {

	public abstract void execute() throws BlueDbException;
	
	@Override
	public void run() {
		try {
			execute();
		} catch (DuplicateKeyException e) {
			throw new RuntimeException("tried to insert duplicate key: " + e.getKey());
		} catch (Throwable t) {
			t.printStackTrace();
			// TODO how to handle failures?
		}
	}

}
