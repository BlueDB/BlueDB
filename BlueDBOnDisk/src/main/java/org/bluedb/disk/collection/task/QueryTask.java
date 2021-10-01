package org.bluedb.disk.collection.task;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.exceptions.DuplicateKeyException;

public abstract class QueryTask implements Runnable {

	public abstract void execute() throws BlueDbException;

	@Override
	public void run() {
		try {
			execute();
		} catch (DuplicateKeyException e) {
			throw new RuntimeException("tried to insert duplicate key: " + e.getKey());
		} catch (BlueDbException e) {
			e.printStackTrace();
			throw new RuntimeException("error executing " + this, e);
		}
	}

}
