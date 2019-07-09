package org.bluedb.api;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.HashGroupedKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.TimeKey;

public class SegmentSize<K extends BlueKey> {
	public static final SegmentSize<TimeKey> TIME_1_HOUR = new SegmentSize<>("TIME_1_HOUR");
	public static final SegmentSize<TimeKey> TIME_2_HOURS = new SegmentSize<>("TIME_2_HOURS");
	public static final SegmentSize<TimeKey> TIME_6_HOURS = new SegmentSize<>("TIME_6_HOURS");
	public static final SegmentSize<TimeKey> TIME_12_HOURS = new SegmentSize<>("TIME_12_HOURS");
	
	public static final SegmentSize<TimeKey> TIME_1_DAY = new SegmentSize<>("TIME_1_DAY");
	public static final SegmentSize<TimeKey> TIME_5_DAYS = new SegmentSize<>("TIME_5_DAYS");
	public static final SegmentSize<TimeKey> TIME_15_DAYS = new SegmentSize<>("TIME_15_DAYS");
	
	public static final SegmentSize<TimeKey> TIME_1_MONTH = new SegmentSize<>("TIME_1_MONTH");
	public static final SegmentSize<TimeKey> TIME_3_MONTHS = new SegmentSize<>("TIME_3_MONTHS");
	public static final SegmentSize<TimeKey> TIME_6_MONTHS = new SegmentSize<>("TIME_6_MONTHS");
	
	public static final SegmentSize<IntegerKey> INT_128 = new SegmentSize<>("INT_128");
	public static final SegmentSize<IntegerKey> INT_256 = new SegmentSize<>("INT_256");
	public static final SegmentSize<IntegerKey> INT_512 = new SegmentSize<>("INT_512");
	public static final SegmentSize<IntegerKey> INT_1K = new SegmentSize<>("INT_1K");
	
	public static final SegmentSize<LongKey> LONG_128 = new SegmentSize<>("LONG_128");
	public static final SegmentSize<LongKey> LONG_256 = new SegmentSize<>("LONG_256");
	public static final SegmentSize<LongKey> LONG_512 = new SegmentSize<>("LONG_512");
	public static final SegmentSize<LongKey> LONG_1K = new SegmentSize<>("LONG_1K");
	
	public static final SegmentSize<HashGroupedKey<?>> HASH_256K = new SegmentSize<>("HASH_256K");
	public static final SegmentSize<HashGroupedKey<?>> HASH_512K = new SegmentSize<>("HASH_512K");
	public static final SegmentSize<HashGroupedKey<?>> HASH_1M = new SegmentSize<>("HASH_1M");
	public static final SegmentSize<HashGroupedKey<?>> HASH_2M = new SegmentSize<>("HASH_2M");
	public static final SegmentSize<HashGroupedKey<?>> HASH_4M = new SegmentSize<>("HASH_4M");
	
	private final String name;

	public SegmentSize(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
}
