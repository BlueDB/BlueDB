package org.bluedb.api;

import java.util.HashMap;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.HashGroupedKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;

/**
 * This class contains public static final variables representing the segment size options available to BlueDB users.
 * It is typed in order to ensure that the selected segment size is compatible with the key type of the {@link BlueCollection}.
 * 
 * <br/><br/>
 * 
 * Think of a collection as an on disk {@link HashMap}. Values are stored in a segment based on the value's grouping number. 
 * If only one value is in a segment then looking up that value by key is constant time. If there are a lot of values in the segment
 * then looking up those values will be slower. However, it is important to limit the number of segments so that you don't end up 
 * using too many i-nodes (files and directories). Large segment sizes will result in more values being stored in each segment.
 * This will result in slower lookup times, slower write speeds, faster retrieval of large amounts of data, and less usage of 
 * i-nodes.
 * 
 * <br/><br/>
 * 
 * 	<ul>
 * 		<li>
 * 			<b>{@link TimeKey} and {@link TimeFrameKey}</b> - Segment sizes are an amount of time for these key types. If you 
 * 			picked TIME_1_HOUR as your segment size then all values that fall within a certain hour will be grouped in the same 
 * 			segment and a new segment will be created for every hour that contains values. Based on the number of values you 
 * 			store per hour you can figure out how fast you will be using i-nodes and how many values will be stored per segment.
 * 		</li>
 * 		<li>
 * 			<b>{@link IntegerKey} and {@link LongKey}</b> - Segment sizes represent how many ids would be stored in each segment. If
 * 			you use an auto incremented long as the id of a LONG_256 segment sized collection then you'd end up with 256 values in
 * 			each segment. Lookup time would still be pretty fast and you would only be using the necessary i-nodes. If you want to
 * 			store a very large number of values you could consider larger segment sizes.
 * 		</li>
 * 		<li>
 * 			<b>{@link UUIDKey} and {@link StringKey}</b> - Segment sizes represent how many hash values would be stored in each
 * 			segment for these key types. If the hashCode method of these was perfect then you'd create a new segment every time 
 * 			you insert a value until the max number of segments and i-nodes is reached. From that point segments would fill up 
 * 			but no additional i-nodes would be used. It is important to pick a segment size that is not going to overuse i-nodes 
 * 			but that still gives you the lookup speed that you need as your collection grows. In the future we might support a
 * 			better mechanism for increasing segment size as the collection grows instead of committing to one size.
 * 		</li>
 * 	</ul>
 * 
 * @param <K> the key type that this segment size is compatible with
 */
public final class SegmentSize<K extends BlueKey> {
	/** All values in each hour time interval will be grouped together in a segment */
	public static final SegmentSize<TimeKey> TIME_1_HOUR = new SegmentSize<>("TIME_1_HOUR");
	/** All values in each 2 hour time interval will be grouped together in a segment */
	public static final SegmentSize<TimeKey> TIME_2_HOURS = new SegmentSize<>("TIME_2_HOURS");
	/** All values in each 6 hour time interval will be grouped together in a segment */
	public static final SegmentSize<TimeKey> TIME_6_HOURS = new SegmentSize<>("TIME_6_HOURS");
	/** All values in each 12 hour time interval will be grouped together in a segment */
	public static final SegmentSize<TimeKey> TIME_12_HOURS = new SegmentSize<>("TIME_12_HOURS");
	
	/** All values in each day time interval will be grouped together in a segment */
	public static final SegmentSize<TimeKey> TIME_1_DAY = new SegmentSize<>("TIME_1_DAY");
	/** All values in each 5 day time interval will be grouped together in a segment */
	public static final SegmentSize<TimeKey> TIME_5_DAYS = new SegmentSize<>("TIME_5_DAYS");
	/** All values in each 15 day time interval will be grouped together in a segment */
	public static final SegmentSize<TimeKey> TIME_15_DAYS = new SegmentSize<>("TIME_15_DAYS");
	
	/** All values in each month time interval will be grouped together in a segment */
	public static final SegmentSize<TimeKey> TIME_1_MONTH = new SegmentSize<>("TIME_1_MONTH");
	/** All values in each 3 month time interval will be grouped together in a segment */
	public static final SegmentSize<TimeKey> TIME_3_MONTHS = new SegmentSize<>("TIME_3_MONTHS");
	/** All values in each 6 month time interval will be grouped together in a segment */
	public static final SegmentSize<TimeKey> TIME_6_MONTHS = new SegmentSize<>("TIME_6_MONTHS");
	
	/** 128 values per segment */
	public static final SegmentSize<IntegerKey> INT_128 = new SegmentSize<>("INT_128");
	/** 256 values per segment */
	public static final SegmentSize<IntegerKey> INT_256 = new SegmentSize<>("INT_256");
	/** 512 values per segment */
	public static final SegmentSize<IntegerKey> INT_512 = new SegmentSize<>("INT_512");
	/** 1K values per segment */
	public static final SegmentSize<IntegerKey> INT_1K = new SegmentSize<>("INT_1K");
	
	/** 128 values per segment */
	public static final SegmentSize<LongKey> LONG_128 = new SegmentSize<>("LONG_128");
	/** 256 values per segment */
	public static final SegmentSize<LongKey> LONG_256 = new SegmentSize<>("LONG_256");
	/** 512 values per segment */
	public static final SegmentSize<LongKey> LONG_512 = new SegmentSize<>("LONG_512");
	/** 1K values per segment */
	public static final SegmentSize<LongKey> LONG_1K = new SegmentSize<>("LONG_1K");
	
	/** 256K hash codes per segment. Max of 16K segments or 41,024 i-nodes (files and directories)*/
	public static final SegmentSize<HashGroupedKey<?>> HASH_256K = new SegmentSize<>("HASH_256K");
	/** 512K hash codes per segment. Max of 8K segments or ~16K i-nodes (files and directories)*/
	public static final SegmentSize<HashGroupedKey<?>> HASH_512K = new SegmentSize<>("HASH_512K");
	/** 1M hash codes per segment. Max of 4K segments or ~8K i-nodes (files and directories)*/
	public static final SegmentSize<HashGroupedKey<?>> HASH_1M = new SegmentSize<>("HASH_1M");
	/** 2M hash codes per segment. Max of 2K segments or ~4K i-nodes (files and directories)*/
	public static final SegmentSize<HashGroupedKey<?>> HASH_2M = new SegmentSize<>("HASH_2M");
	/** 4M hash codes per segment. Max of 1K segments or ~2K i-nodes (files and directories)*/
	public static final SegmentSize<HashGroupedKey<?>> HASH_4M = new SegmentSize<>("HASH_4M");
	/** 8M hash codes per segment. Max of 512 segments or ~1K i-nodes (files and directories)*/
	public static final SegmentSize<HashGroupedKey<?>> HASH_8M = new SegmentSize<>("HASH_8M");
	
	static final SegmentSize<StringKey> INVALID = new SegmentSize<>("Invalid");
	
	private final String name;

	private SegmentSize(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
}
