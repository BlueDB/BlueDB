package org.bluedb.disk.collection;

import static org.junit.Assert.fail;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class CollectionTestTools {

	public static void waitForExecutorToFinish(ReadWriteBlueCollectionOnDisk<?> collection) {
		Runnable doNothing = new Runnable() {@Override public void run() {}};
		Future<?> future = collection.getSharedExecutor().submitQueryTask(collection.getPath().toString(), doNothing);
		try {
			future.get();
		} catch (ExecutionException | InterruptedException e) {
			e.printStackTrace();
			fail();
		}
	}

	public static void waitForExecutorToFinish(ReadWriteBlueTimeCollectionOnDisk<?> collection) {
		Runnable doNothing = new Runnable() {@Override public void run() {}};
		Future<?> future = collection.getSharedExecutor().submitQueryTask(collection.getPath().toString(), doNothing);
		try {
			future.get();
		} catch (ExecutionException | InterruptedException e) {
			e.printStackTrace();
			fail();
		}
	}
}
