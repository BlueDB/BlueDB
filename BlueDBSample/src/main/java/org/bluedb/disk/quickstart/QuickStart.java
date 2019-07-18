package org.bluedb.disk.quickstart;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.BlueDb;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.IntegerIndexKeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.BlueDbOnDiskBuilder;

public class QuickStart {
	
	/*
	 *  We'll create a POJO for our examples.  In a real database, best practice suggests using an 
	 *  interface for the collection’s value type and to version the implementations of that interface 
	 *  so that you can update the schema in the future without breaking serialization.
	 */
	
	public static class Ball implements Serializable {
		private static final long serialVersionUID = 1L;

		private int radius;
		
		public Ball(int radius) {
			this.radius = radius;
		}

		public int getRadius() {
			return radius;
		}

		public void setRadius(int radius) {
			this.radius = radius;
		}
	}
	
	
	//Create an index key extractor that can help BlueDB create an index on the size of the ball
	
	public static class BallSizeIndexKeyExtractor implements IntegerIndexKeyExtractor<Ball> {
		private static final long serialVersionUID = 1L;
		@Override
		public List<Integer> extractIntsForIndex(Ball value) {
			return Arrays.asList(value.getRadius());
		}
	}

	public static void main(String[] args) throws BlueDbException {
		
		//Create a BlueDB instance
		
		Path path = Paths.get("usr", "local", "bluedb", "data");
		BlueDb blueDb = new BlueDbOnDiskBuilder()
				.withPath(path)
				.build();

		
		// Create a collection instance.
		
		BlueCollection<Ball> ballCollection = blueDb.collectionBuilder("ball_collection", TimeKey.class, Ball.class).build();
		
		
		// Insert an object. BlueDB is optimized for time so we’ll use a TimeKey for our example.
		
		TimeKey key = new TimeKey(1, System.currentTimeMillis());
		Ball myBall = new Ball(1);
		ballCollection.insert(key, myBall);
		
		
		// Confirm the object is saved.
		
		ballCollection.contains(key);  // true

		
		// Fetch the object.
		
		ballCollection.get(key);  // ball

		
		// Update the object.
		
		ballCollection.update(key, ball -> ball.setRadius(2));

		
		// Delete the object.
		
		ballCollection.delete(key);

		
		/*
		 *  Batch upsert (inserts or overwrites values at matching keys) can significantly improve performance 
		 *  when inserting many objects into the collection.
		 */
		
		TimeKey key1 = new TimeKey(1, System.currentTimeMillis());
		Ball ball1 = new Ball(1);
		
		TimeKey key2 = new TimeKey(2, System.currentTimeMillis());
		Ball ball2 = new Ball(2);

		Map<BlueKey, Ball> batch = new HashMap<>();
		batch.put(key1, ball1);
		batch.put(key2, ball2);
		ballCollection.batchUpsert(batch);

		
		// Query objects mapped to a TimeKey within the last hour, meeting a filter.
		
		long now = System.currentTimeMillis();
		long oneHourAgo = now - 60 * 60 * 1000;
		List<Ball> ballsOfRadius2= ballCollection.query()
			.afterTime(oneHourAgo)
			.beforeOrAtTime(now)
			.where(ball -> ball.getRadius() == 2)
			.getList();
		
		
		// Delete all objects meeting some filter.
		
		ballCollection.query()
			.where(ball -> ball.getRadius() == 2)
			.delete();

		
		// Update all objects meeting some filter.
		
		ballCollection.query()
			.where(ball -> ball.getRadius() == 2)
			.update(ball -> ball.setRadius(3));
		
		
		//Create an index on ball size 
		
		BlueIndex<IntegerKey, Ball> ballSizeIndex = ballCollection.createIndex("ball_size_index", IntegerKey.class, new BallSizeIndexKeyExtractor());
		
		
		// Insert a value and then retrieve it using the index.
		
		TimeKey key3 = new TimeKey(3, System.currentTimeMillis());
		Ball ball3 = new Ball(7);
		ballCollection.insert(key, ball3);
		List<Ball> ballsOfSize7 = ballSizeIndex.get(new IntegerKey(7));  // returns List containing ball3;

	}
}
