package org.bluedb.disk.segment;

import static java.util.Arrays.asList;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.FIFTEEN_DAYS;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.FIVE_DAYS;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.ONE_DAY;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.ONE_HOUR;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.ONE_MILLI;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.ONE_MONTH;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.SIX_HOURS;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.SIX_MONTHS;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.SIX_SECONDS;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.THREE_MONTHS;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.TWELVE_HOURS;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.TWO_HOURS;

import java.util.List;

import org.bluedb.api.SegmentSize;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.HashGroupedKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.segment.path.SegmentSizeConfiguration;

/*
 * Serialized in collection meta data. Do NOT move the enum to a new package, remove an enum constant, or change the ordering. 
 * You may add enum constants to the end. You may also update the config of an enum constant as long as it is backwards compatible.
 * For example, you could add a compatible roll-up level.
 * 
 * Time Collections - I-nodes are created according to the times on values inserted into collection. If a collection will span a very large timeframe 
 * then a larger segment size is recommended in order to use less i-nodes. Especially if a value has start and end times that are far apart.
 * 
 * Int, Long Collections - These collections expect to be inserted in order. Segments will be created and filled to capacity as your data set grows.
 * Therefore i-node usage shouldn't get out of control unless you have a huge data set. If you need to support a huge data set without using as many 
 * i-nodes and you are okay to sacrifice some lookup performance then you can choose a larger segment size. 
 * 
 * Hash Grouped (UUID, String) - These values are inserted into collections in random order. I-nodes need to be capped in order to stop them from
 * growing out of control for small data sets. Segments will be created quickly and will slowly fill up.
 */
public enum SegmentSizeSetting {
	TIME_1_HOUR(new SegmentSizeConfiguration(TimeKey.class, 	asList(ONE_HOUR, 	24L, 30L, 12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR))),
	TIME_2_HOURS(new SegmentSizeConfiguration(TimeKey.class, 	asList(TWO_HOURS, 	12L, 30L, 12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR, TWO_HOURS))),
	TIME_6_HOURS(new SegmentSizeConfiguration(TimeKey.class, 	asList(SIX_HOURS, 	 4L, 30L, 12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR, SIX_HOURS))),
	TIME_12_HOURS(new SegmentSizeConfiguration(TimeKey.class, 	asList(TWELVE_HOURS, 2L, 30L, 12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR, TWELVE_HOURS))),
	TIME_1_DAY(new SegmentSizeConfiguration(TimeKey.class, 		asList(ONE_DAY, 	     30L, 12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR, ONE_DAY))),
	TIME_5_DAYS(new SegmentSizeConfiguration(TimeKey.class, 	asList(FIVE_DAYS, 		  6L, 12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR, ONE_DAY, FIVE_DAYS))),
	TIME_15_DAYS(new SegmentSizeConfiguration(TimeKey.class,	asList(FIFTEEN_DAYS, 	  2L, 12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR, ONE_DAY, FIFTEEN_DAYS))),
	TIME_1_MONTH(new SegmentSizeConfiguration(TimeKey.class, 	asList(ONE_MONTH, 			  12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR, ONE_DAY, ONE_MONTH))),
	TIME_3_MONTHS(new SegmentSizeConfiguration(TimeKey.class,	asList(THREE_MONTHS, 		  12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR, ONE_DAY, ONE_MONTH, THREE_MONTHS))),
	TIME_6_MONTHS(new SegmentSizeConfiguration(TimeKey.class, 	asList(SIX_MONTHS, 			  12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR, ONE_DAY, ONE_MONTH, SIX_MONTHS))),
	
	INT_128(new SegmentSizeConfiguration(IntegerKey.class, 	asList(128L,  128L, 64L, 64L), 	asList(1L, 128L))),
	INT_256(new SegmentSizeConfiguration(IntegerKey.class, 	asList(256L,   64L, 64L, 64L), 	asList(1L, 256L))), //Original, Default
	INT_512(new SegmentSizeConfiguration(IntegerKey.class, 	asList(512L,   32L, 64L, 64L), 	asList(1L, 512L))), //Index Default
	INT_1K(new SegmentSizeConfiguration(IntegerKey.class, 	asList(1024L,  16L, 64L, 64L), 	asList(1L, 1024L))),
	INT_2K(new SegmentSizeConfiguration(IntegerKey.class, 	asList(2048L,   8L, 64L, 64L), 	asList(1L, 2048L))),
	INT_4K(new SegmentSizeConfiguration(IntegerKey.class, 	asList(4096L,   4L, 64L, 64L), 	asList(1L, 4096L))),
	INT_8K(new SegmentSizeConfiguration(IntegerKey.class, 	asList(8192L,   2L, 64L, 64L), 	asList(1L, 8192L))),
	INT_16K(new SegmentSizeConfiguration(IntegerKey.class, 	asList(16384L,      64L, 64L), 	asList(1L, 16384L))),
	INT_32K(new SegmentSizeConfiguration(IntegerKey.class, 	asList(32768L,      32L, 64L), 	asList(1L, 32768L))),
	
	/*
	 * The group number algorithm for LongKey results in two longs per grouping number. Therefore, a segment size of 64 grouping 
	 * numbers results in 128 values being stored per segment. We are naming them based on the number of values in each segment 
	 * so that it matches the IntegerKey naming. That is why LONG_128 has a segment size of 64.
	 */
	LONG_128(new SegmentSizeConfiguration(LongKey.class, 	asList(64L,   256L, 512L, 512L, 512L, 256L, 128L), 	asList(1L, 64L))), //Original
	LONG_256(new SegmentSizeConfiguration(LongKey.class, 	asList(128L,  128L, 512L, 512L, 512L, 256L, 128L), 	asList(1L, 128L))), //Default
	LONG_512(new SegmentSizeConfiguration(LongKey.class, 	asList(256L,   64L, 512L, 512L, 512L, 256L, 128L), 	asList(1L, 256L))), //Index Default
	LONG_1K(new SegmentSizeConfiguration(LongKey.class, 	asList(512L,   32L, 512L, 512L, 512L, 256L, 128L), 	asList(1L, 512L))),
	LONG_2K(new SegmentSizeConfiguration(LongKey.class, 	asList(1024L,  16L, 512L, 512L, 512L, 256L, 128L), 	asList(1L, 1024L))),
	LONG_4K(new SegmentSizeConfiguration(LongKey.class, 	asList(2048L,   8L, 512L, 512L, 512L, 256L, 128L), 	asList(1L, 2048L))),
	LONG_8K(new SegmentSizeConfiguration(LongKey.class, 	asList(4096L,   4L, 512L, 512L, 512L, 256L, 128L), 	asList(1L, 4096L))),
	LONG_16K(new SegmentSizeConfiguration(LongKey.class, 	asList(8192L,   2L, 512L, 512L, 512L, 256L, 128L), 	asList(1L, 8192L))),
	LONG_32K(new SegmentSizeConfiguration(LongKey.class, 	asList(16384L,      512L, 512L, 512L, 256L, 128L), 	asList(1L, 16384L))),
	
	HASH_1K(new SegmentSizeConfiguration(HashGroupedKey.class, 		asList(1024L,    512L, 128L, 64L), 	asList(1L, 1024L))), //4M Segments
	HASH_2K(new SegmentSizeConfiguration(HashGroupedKey.class, 		asList(2048L,    256L, 128L, 64L), 	asList(1L, 2048L))), //2M Segments
	HASH_4K(new SegmentSizeConfiguration(HashGroupedKey.class, 		asList(4096L,    128L, 128L, 64L), 	asList(1L, 4096L))), //1M Segments
	HASH_8K(new SegmentSizeConfiguration(HashGroupedKey.class, 		asList(8192L,     64L, 128L, 64L),  asList(1L, 8192L))), //512K Segments
	HASH_16K(new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(16384L,    32L, 128L, 64L),  asList(1L, 16384L))), //256K Segments
	HASH_32K(new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(32768L,    16L, 128L, 64L),  asList(1L, 32768L))), //128K Segments
	HASH_64K(new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(65536L,     8L, 128L, 64L),  asList(1L, 65536L))), //64K Segments
	HASH_128K(new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(131072L,    4L, 128L, 64L),  asList(1L, 131072L))), //32K Segments
	HASH_256K(new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(262144L,    2L, 128L, 64L),  asList(1L, 262144L))), //16K Segments
	HASH_512K(new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(524288L,        128L, 64L),  asList(1L, 524288L))), //8K Segments Original
	HASH_1M(new SegmentSizeConfiguration(HashGroupedKey.class,		asList(1048576L,        64L, 64L),  asList(1L, 1048576L))), //4K Segments Default
	HASH_2M(new SegmentSizeConfiguration(HashGroupedKey.class, 		asList(2097152L,        32L, 64L),  asList(1L, 2097152L))), //2K Segments Index Default
	HASH_4M(new SegmentSizeConfiguration(HashGroupedKey.class,		asList(4194304L,        16L, 64L),  asList(1L, 4194304L))), //1K Segments
	HASH_8M(new SegmentSizeConfiguration(HashGroupedKey.class, 		asList(8388608L,         8L, 64L),	asList(1L, 8388608L))), //512 Segments
	HASH_16M(new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(16777216L,        4L, 64L),  asList(1L, 16777216L))), //256 Segments
	HASH_32M(new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(33554432L,        2L, 64L),  asList(1L, 33554432L))), //128 Segments
	;
	
	private SegmentSizeConfiguration config;
	
	SegmentSizeSetting(SegmentSizeConfiguration config) {
		this.config = config;
	}
	
	public SegmentSizeConfiguration getConfig() {
		return config;
	}

	public long getSegmentSize() {
		return config.getSegmentSize();
	}

	public List<Long> getRollupSizes() {
		return config.getRollupsBottomToTop();
	}

	public List<Long> getFolderSizes() {
		return config.getFolderSizesTopToBottom();
	}

	public static SegmentSizeSetting getDefaultIndexSettingsFor(Class<? extends BlueKey> keyType) throws BlueDbException {
		if (TimeKey.class.isAssignableFrom(keyType)) {
			return TIME_1_DAY;
		} else if (LongKey.class.isAssignableFrom(keyType)) {
			return LONG_512;
		} else if (IntegerKey.class.isAssignableFrom(keyType)) {
			return INT_512;
		} else if (HashGroupedKey.class.isAssignableFrom(keyType)) {
			return HASH_2M;
		} else {
			throw new BlueDbException("No " + SegmentSizeSetting.class.getSimpleName() + " for " + keyType);
		}
	}

	public static SegmentSizeSetting getDefaultSettingsFor(Class<? extends BlueKey> keyType) throws BlueDbException {
		if (TimeKey.class.isAssignableFrom(keyType)) {
			return TIME_1_HOUR;
		} else if (LongKey.class.isAssignableFrom(keyType)) {
			return LONG_256;
		} else if (IntegerKey.class.isAssignableFrom(keyType)) {
			return INT_256;
		} else if (HashGroupedKey.class.isAssignableFrom(keyType)) {
			return HASH_1M;
		} else {
			throw new BlueDbException("No " + SegmentSizeSetting.class.getSimpleName() + " for " + keyType);
		}
	}

	public static SegmentSizeSetting getOriginalDefaultSettingsFor(Class<? extends BlueKey> keyType) throws BlueDbException {
		if (TimeKey.class.isAssignableFrom(keyType)) {
			return TIME_1_HOUR;
		} else if (LongKey.class.isAssignableFrom(keyType)) {
			return LONG_128;
		} else if (IntegerKey.class.isAssignableFrom(keyType)) {
			return INT_256;
		} else if (HashGroupedKey.class.isAssignableFrom(keyType)) {
			return HASH_512K;
		} else {
			throw new BlueDbException("No original " + SegmentSizeSetting.class.getSimpleName() + " for " + keyType);
		}
	}

	public static <K extends BlueKey> SegmentSizeSetting fromUserSelection(SegmentSize<K> requestedSegmentSize) throws BlueDbException {
		try {
			return SegmentSizeSetting.valueOf(requestedSegmentSize.getName());
		} catch(Throwable t) {
			throw new BlueDbException("No " + SegmentSizeSetting.class.getSimpleName() + " for requested segment size " + requestedSegmentSize, t);
		}
	}
}
