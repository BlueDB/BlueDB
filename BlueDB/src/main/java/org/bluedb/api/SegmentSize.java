package org.bluedb.api;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.HashGroupedKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.TimeKey;

public class SegmentSize<T extends BlueKey> {

	public static final SegmentSize<TimeKey> TIME_1_HOUR = new SegmentSize<>(3_600_000L);
	public static final SegmentSize<LongKey> LONG_DEFAULT = new SegmentSize<>(64L);
	public static final SegmentSize<IntegerKey> INT_DEFAULT = new SegmentSize<>(256L);
	public static final SegmentSize<HashGroupedKey<?>> HASH_DEFAULT = new SegmentSize<>(524288L);

	private final long size;
	
	private SegmentSize(long size) {
		this.size = size;
	}

	public long getSize() {
		return size;
	}

}
