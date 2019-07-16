package org.bluedb.disk.segment.path;

public class SegmentPathTimeUnits {
	public static long ONE_MILLI = 1L;
	public static long SIX_SECONDS = 6_000L;
	public static long ONE_MINUTE = 60_000L;
	
	public static long ONE_HOUR = 3_600_000L;
	public static long TWO_HOURS = ONE_HOUR * 2;
	public static long SIX_HOURS = ONE_HOUR * 6;
	public static long TWELVE_HOURS = ONE_HOUR * 12;
	
	public static long ONE_DAY = ONE_HOUR * 24;
	public static long FIVE_DAYS = ONE_DAY * 5;
	public static long FIFTEEN_DAYS = ONE_DAY * 15;
	
	public static long ONE_MONTH = ONE_DAY * 30;
	public static long THREE_MONTHS = ONE_MONTH * 3;
	public static long SIX_MONTHS = ONE_MONTH * 6;
}
