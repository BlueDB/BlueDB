package io.bluedb.disk;

import java.util.concurrent.TimeUnit;

public class SegmentIdConverter {
	private static final long MILLIS_IN_BLOCK = TimeUnit.HOURS.toMillis(1);

	public static long convertTimeToSegmentId(long time) {
		return time / MILLIS_IN_BLOCK;
	}

	public static long convertSegmentIdToTime(long segmentId) {
		return segmentId * MILLIS_IN_BLOCK;
	}
}
