package io.bluedb.disk.segment;

public class TimeRange {

	@Override
	public String toString() {
		return "TimeRange [start=" + start + ", end=" + end + "]";
	}

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
			long end = Long.valueOf(parts[0]);
			return new TimeRange(start, end);
		} catch (Throwable t) {
			// TODO throw exception ?
			return null;
		}
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
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TimeRange other = (TimeRange) obj;
		if (end != other.end)
			return false;
		if (start != other.start)
			return false;
		return true;
	}
}
