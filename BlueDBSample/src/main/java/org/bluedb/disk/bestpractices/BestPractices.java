package org.bluedb.disk.bestpractices;

import java.awt.Color;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.BlueDb;
import org.bluedb.api.BlueQuery;
import org.bluedb.api.CloseableIterator;
import org.bluedb.api.SegmentSize;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.IntegerIndexKeyExtractor;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.BlueDbOnDiskBuilder;

public class BestPractices {

	public static void main(String[] args) throws BlueDbException, IOException {
		BlueDb blueDb = useABuilderForBlueDB();
		
		List<Class<? extends Serializable>> classesToRegister = Arrays.asList(BallV1.class, BallV2.class, Color.class);
		
		BlueCollection<Ball> ballCollection = useABuilderForCollections(blueDb, classesToRegister);
		
		registerClassesWithCollection(blueDb);

		useCloseableIterator(ballCollection);
		
		useFileSystemFriendlyNames(blueDb, ballCollection);
	}
	
	//IGNORE ABOVE THIS LINE
	
	/*
	 * BlueDB uses FST in order to serialize and deserialize objects very quickly. However, it is much easier
	 * to break serialization with FST than with standard java serialization. For this reason it is recommended
	 * to use an interface as the type of a collection. New data can be added to the interface and new versions
	 * or implementations of the data can be created and stored in the database along side the old versions.
	 * 
	 * As an example we'll use a simple interface called Ball. When the ball collection was first created, it only
	 * had a radius. Later color was added. Thus there are two versions of Balls in the collection. V1 only has a
	 * radius, and V2 has a radius and a Color.
	 */
	public interface Ball extends Serializable {
		public int getRadius();
		public void setRadius(int radius);
		
		public Color getColor();
		public void setColor(Color color);
		
		public Ball createMostRecentVersion();
	}
	
	
	/*
	 * BallV1 is the first version of Ball and it must remain on the classpath as long as the collection
	 * contains values of this version. Note that it does not have Color data because it existed before Color 
	 * data was added to the ball collection. For backwards compatibility the getColor() and setColor()
	 * methods were added. The color defaults to white and can't be changed. You will need to replace a BallV1 with a 
	 * BallV2 if you wish to set the color to something else. 
	 * 
	 * In the future BlueDB might support a better way to upgrade values to the latest version before updates are
	 * applied in order to allow for simple and lazy upgrading of values.
	 * 
	 * [Website Note] Might want to make it easy to collapse the details of this class or something so it doesn't take up too much space.
	 */
	public static class BallV1 implements Ball {
		private int radius;
		
		public BallV1(int radius) {
			this.radius = radius;
		}

		@Override
		public int getRadius() {
			return radius;
		}

		@Override
		public void setRadius(int radius) {
			this.radius = radius;
		}

		@Override
		public Color getColor() { 
			return Color.WHITE; 
		}

		@Override
		public void setColor(Color color) { 
		}

		@Override
		public Ball createMostRecentVersion() {
			return new BallV2(radius, Color.WHITE);
		}
	}
	
	
	/*
	 * BallV2 was created because the ball collection needed Color information.
	 * 
	 * [Website Note] Might want to make it easy to collapse the details of this class or something so it doesn't take up too much space.
	 */
	public static class BallV2 implements Ball {
		private int radius;
		private Color color;
		
		public BallV2(int radius, Color color) {
			this.radius = radius;
			this.color = color;
		}

		@Override
		public int getRadius() {
			return radius;
		}

		@Override
		public void setRadius(int radius) {
			this.radius = radius;
		}

		@Override
		public Color getColor() { 
			return color; 
		}

		@Override
		public void setColor(Color color) { 
			this.color = color;
		}

		@Override
		public Ball createMostRecentVersion() {
			return null; //This is the most recent version
		}
	}
	
	
	/*
	 *  Index key extractors get serialized to disk by BlueDB so you should define them in their own
	 *  class to avoid serialization issues. Stay away from anonymous inner classes and especially 
	 *  avoid using lambdas as key extractors.
	 */
	public static class BallSizeIndexKeyExtractor implements IntegerIndexKeyExtractor<Ball> {
		private static final long serialVersionUID = 1L;
		@Override
		public List<Integer> extractIntsForIndex(Ball ball) {
			return Arrays.asList(ball.getRadius());
		}
	}
	

	// Use a builder for BlueDb objects
	private static BlueDb useABuilderForBlueDB() {
		BlueDb blueDb = new BlueDbOnDiskBuilder()
				.withPath(Paths.get("bluedb"))
				.build();
		return blueDb; //[Line not needed on website]
	}

	// Use a builder for BlueCollection objects
	private static BlueCollection<Ball> useABuilderForCollections(BlueDb blueDb, List<Class<? extends Serializable>> classesToRegister) throws BlueDbException {
		BlueCollection<Ball> ballCollection = blueDb.collectionBuilder("ball_collection", TimeKey.class, Ball.class)
				.withOptimizedClasses(classesToRegister)
				.withSegmentSize(SegmentSize.TIME_1_DAY)
				.build();
		return ballCollection; //[Line not needed on website]
	}

	/*
	 * Register the types of objects that will be serialized in this collection. This uses less disk space
	 * and allows BlueDB to query the collection faster. The reason is that FST will serialize the full
	 * canonical name (e.g. "com.package.subpackage.MySerializedClass") each time the objects are serialized.
	 * Registering the classes that will be serialized allows FST to save the type of each object as an integer
	 * instead of as a large string. 
	 * 
	 * Note that the collection value type, standard BlueKey types, and standard java collections are 
	 * automatically registered
	 * 
	 * Note that any class registered in this way needs to remain on the classpath forever in order for this 
	 * collection to function. In the future we may be able to make a way to unregister a class.
	 */
	private static void registerClassesWithCollection(BlueDb blueDb) throws BlueDbException {
		List<Class<? extends Serializable>> classesToRegister = Arrays.asList(BallV1.class, BallV2.class, Color.class);
		
		BlueCollection<Ball> collection = blueDb.collectionBuilder("ball-collection", TimeKey.class, Ball.class)
				.withOptimizedClasses(classesToRegister) //registers classes
				.build();
	}

	/*
	 *  Use a CloseableIterator when running a large query to keep memory usage down. Put the iterator 
	 *  in a try-with-resources to ensure that it automatically closes. Leaving an iterator open too long
	 *  may keep files open and hold locks on resources which could block other queries from completing.
	 *  If you fail to call next on the iterator quickly enough it will timeout and release the locks. 
	 *  The default timeout is 15 seconds.
	 */
	private static void useCloseableIterator(BlueCollection<Ball> ballCollection) throws IOException, BlueDbException {
		BlueQuery<Ball> query = ballCollection.query()
				.where(o -> o.getRadius() == 2);

		try (CloseableIterator<Ball> iterator = query.getIterator()) {
			while(iterator.hasNext()) {
				Ball ball = iterator.next();
				//Process ball
			}
		}
	}

	// Use file-system friendly names for BlueCollections and BlueIndexes
	private static void useFileSystemFriendlyNames(BlueDb blueDb, BlueCollection<Ball> ballCollection) throws BlueDbException {
		//Good
		BlueCollection<Ball> ballCollection1 = blueDb.collectionBuilder("ball_collection", TimeKey.class, Ball.class).build();
		BlueIndex<IntegerKey, Ball> myIndex1 = ballCollection.createIndex("ball-size-index", IntegerKey.class, new BallSizeIndexKeyExtractor());

		//Bad
		BlueCollection<Ball> ballCollection2 = blueDb.collectionBuilder("Don't do this", TimeKey.class, Ball.class).build();
		BlueIndex<IntegerKey, Ball> myIndex2 = ballCollection.createIndex("// or // this", IntegerKey.class, new BallSizeIndexKeyExtractor());
	}
}
