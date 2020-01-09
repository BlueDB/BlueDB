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
import org.bluedb.api.BlueTimeCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.IntegerIndexKeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.BlueDbOnDiskBuilder;

public class QuickStart {
	
	public static void main(String[] args) throws BlueDbException {
		Path path = Paths.get("usr", "local", "bluedb", "data");
		
		BlueDb blueDb = createBlueDbInstance(path);
		
		BlueTimeCollection<Ball> ballCollection = createCollectionInstance(blueDb);
		
		TimeKey key = insertValueUsingTime(ballCollection);
		
		confirmValueSaved(ballCollection, key);
		
		fetchValue(ballCollection, key);
		
		updateValue(ballCollection, key);
		
		deleteValue(ballCollection, key);
		
		batchUpsert(ballCollection);
		
		queryValuesInTimeframeMatchingCondition(ballCollection);
		
		deleteAllValuesMeetingCondition(ballCollection);
		
		updateAllValuesMatchingCondition(ballCollection);
		
		BlueIndex<IntegerKey, Ball> ballSizeIndex = createIndexOnBallSize(ballCollection);
		
		insertValueAndRetreiveViaIndex(ballCollection, ballSizeIndex);
	}
	
	//IGNORE ABOVE HERE
	
	/*
	 *  For this quick start we'll persist a POJO called ball that has a radius property. See best practices
	 *  for more information about how to structure your database model objects.
	 */
	public static class Ball implements Serializable {
		private int radius;
		//[Hide below here in a ... Maybe give them access to expand to see the details or a link or something]
		public Ball(int radius) {
			this.radius = radius;
		}

		public int getRadius() {
			return radius;
		}

		public void setRadius(int radius) {
			this.radius = radius;
		}
		//[Hide above here in a ...]
	}

	//Create a BlueDB instance
	private static BlueDb createBlueDbInstance(Path path) {
		BlueDb blueDb = new BlueDbOnDiskBuilder()
				.withPath(path)
				.build();
		return blueDb; //[Line not needed on website]
	}

	// Create a collection instance.
	private static BlueTimeCollection<Ball> createCollectionInstance(BlueDb blueDb) throws BlueDbException {
		BlueTimeCollection<Ball> ballCollection = blueDb.getTimeCollectionBuilder("ball_collection", TimeKey.class, Ball.class).build();
		return ballCollection; //[Line not needed on website]
	}

	// Insert an object. BlueDB is optimized for time so we’ll use a TimeKey for our example.
	private static TimeKey insertValueUsingTime(BlueCollection<Ball> ballCollection) throws BlueDbException {
		TimeKey key = new TimeKey(1, System.currentTimeMillis());
		Ball ball = new Ball(1);
		ballCollection.insert(key, ball);
		return key; //[Line not needed on website]
	}

	// Confirm the value is saved
	private static void confirmValueSaved(BlueCollection<Ball> ballCollection, TimeKey key) throws BlueDbException {
		boolean contains = ballCollection.contains(key);
	}

	// Fetch the value
	private static void fetchValue(BlueCollection<Ball> ballCollection, TimeKey key) throws BlueDbException {
		Ball ball = ballCollection.get(key);
	}

	// Update the value
	private static void updateValue(BlueCollection<Ball> ballCollection, TimeKey key) throws BlueDbException {
		ballCollection.update(key, ball -> ball.setRadius(2));
	}

	// Delete the value
	private static void deleteValue(BlueCollection<Ball> ballCollection, TimeKey key) throws BlueDbException {
		ballCollection.delete(key);
	}

	/*
	 *  Batch upsert (inserts or overwrites key/value pairs) can significantly improve performance 
	 *  when inserting many objects into the collection rather than one at a time.
	 */
	private static void batchUpsert(BlueCollection<Ball> ballCollection) throws BlueDbException {
		TimeKey key1 = new TimeKey(1, System.currentTimeMillis());
		Ball ball1 = new Ball(1);
		
		TimeKey key2 = new TimeKey(2, System.currentTimeMillis());
		Ball ball2 = new Ball(2);

		Map<BlueKey, Ball> batch = new HashMap<>();
		batch.put(key1, ball1);
		batch.put(key2, ball2);
		ballCollection.batchUpsert(batch);
	}

	// Query objects mapped to a TimeKey within the last hour, meeting a filter.
	private static void queryValuesInTimeframeMatchingCondition(BlueTimeCollection<Ball> ballCollection) throws BlueDbException {
		long now = System.currentTimeMillis();
		long oneHourAgo = now - 60 * 60 * 1000;
		List<Ball> ballsOfRadius2= ballCollection.query()
			.afterTime(oneHourAgo)
			.beforeOrAtTime(now)
			.where(ball -> ball.getRadius() == 2)
			.getList();
	}

	// Delete all values meeting some condition
	private static void deleteAllValuesMeetingCondition(BlueCollection<Ball> ballCollection) throws BlueDbException {
		ballCollection.query()
			.where(ball -> ball.getRadius() == 2)
			.delete();
	}

	// Update all values meeting some condition
	private static void updateAllValuesMatchingCondition(BlueCollection<Ball> ballCollection) throws BlueDbException {
		ballCollection.query()
			.where(ball -> ball.getRadius() == 2)
			.update(ball -> ball.setRadius(3));
	}
	
	/*
	 * Add an index on ball size to the collection. This index key extractor allows BlueDB to create an
	 * index key based on the radius of any ball. See best practices for details on creating index key extractors.
	 */
	
	//[Website note] This whole class definition should be included, not just the body
	public static class BallSizeIndexKeyExtractor implements IntegerIndexKeyExtractor<Ball> {
		@Override
		public List<Integer> extractIntsForIndex(Ball value) {
			return Arrays.asList(value.getRadius());
		}
	}
	
	//[Website note] The body of this method should be in its own code block but it belongs with the same description of the above class
	private static BlueIndex<IntegerKey, Ball> createIndexOnBallSize(BlueCollection<Ball> ballCollection) throws BlueDbException {
		BlueIndex<IntegerKey, Ball> ballSizeIndex = ballCollection.createIndex("ball_size_index", IntegerKey.class, new BallSizeIndexKeyExtractor());
		return ballSizeIndex; //[Line not needed on website]
	}

	// Insert a value and then retrieve it using the index
	private static void insertValueAndRetreiveViaIndex(BlueCollection<Ball> ballCollection, BlueIndex<IntegerKey, Ball> ballSizeIndex) throws BlueDbException {
		TimeKey key = new TimeKey(3, System.currentTimeMillis());
		Ball ball = new Ball(7);
		ballCollection.insert(key, ball);
		List<Ball> ballsOfSize7 = ballSizeIndex.get(new IntegerKey(7));  // returns List containing the ball;
	}
}
