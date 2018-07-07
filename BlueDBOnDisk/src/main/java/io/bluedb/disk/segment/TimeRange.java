package io.bluedb.disk.segment;

public final class TimeRange implements Comparable<TimeRange> {

	private final long start;
	private final long end;
	
	public TimeRange(long start, long end) {
		this.start = start;
		this.end = end;
	}

	public long getStart() {
		return start;
	}

	public long getEnd() {
		return end;
	}

	public String toUnderscoreDelimitedString() {
		return start + "_" + end;
	}

	public static TimeRange fromUnderscoreDelmimitedString(String string) {
		try {
			String[] parts = string.split("_");
			long start = Long.valueOf(parts[0]);
			long end = Long.valueOf(parts[1]);
			return new TimeRange(start, end);
		} catch (Throwable t) {
			return null;
		}
	}

	@Override
	public String toString() {
		return "TimeRange [start=" + start + ", end=" + end + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (end ^ (end >>> 32));
		result = prime * result + (int) (start ^ (start >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TimeRange) {
			TimeRange other = (TimeRange) obj;
			return (end == other.end) && (start == other.start); 
		} else {
			return false;
		}
	}

	@Override
	public int compareTo(TimeRange other) {
		if (this.start > other.start) {
			return 1;
		} else if (this.start < other.start) {
			return -1;
		} else {
			if (this.end > other.end) {
				return 1;
			} else if (this.end < other.end){
				return -1;
			} else {
				return 0;
			}
		}
	}
}
