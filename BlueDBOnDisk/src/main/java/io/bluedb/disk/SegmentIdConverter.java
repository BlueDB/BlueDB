package io.bluedb.disk;

import java.util.Calendar;
import java.util.concurrent.TimeUnit;

public class SegmentIdConverter {
	private static final long MILLIS_IN_BLOCK = TimeUnit.HOURS.toMillis(1);
	private static final long epoch = findEpoch();

	public long convertTimeToSegmentId(long time) {
		return (time - epoch) / MILLIS_IN_BLOCK;
	}

	public long convertSegmentIdToTime(long segmentId) {
		return epoch + (segmentId * MILLIS_IN_BLOCK);
	}

	private static long findEpoch() {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(0);
		cal.add(Calendar.DAY_OF_YEAR, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);

		return cal.getTimeInMillis();
	}
}
