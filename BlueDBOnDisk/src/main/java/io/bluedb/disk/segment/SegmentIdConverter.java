package io.bluedb.disk.segment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SegmentIdConverter {
	public static final long MILLIS_IN_BLOCK = TimeUnit.HOURS.toMillis(1);

	public static long convertTimeToSegmentId(long time) {
		return time / MILLIS_IN_BLOCK;
	}

	public static long convertSegmentIdToTime(long segmentId) {
		return segmentId * MILLIS_IN_BLOCK;
	}

	public static List<Long> getSegments(long min, long max) {
		List<Long> segments = new ArrayList<>();
		long i = Math.min(min, max);
		while (i <= Math.max(min, max)) {
			segments.add(convertTimeToSegmentId(i));
			i += MILLIS_IN_BLOCK;
		}
		return segments;
	}
}
