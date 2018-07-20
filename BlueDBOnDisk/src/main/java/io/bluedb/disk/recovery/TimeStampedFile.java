package io.bluedb.disk.recovery;

import java.io.File;

public class TimeStampedFile implements Comparable<TimeStampedFile> {
	private final Long timestamp;
	private final File file;

	public TimeStampedFile (File file) {
		this.file = file;
		this.timestamp = extractTimestamp(file);
	}

	public File getFile() {
		return file;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	@Override
	public int compareTo(TimeStampedFile o) {
		return timestamp.compareTo(o.timestamp);
	}

	public static Long extractTimestamp(File file) {
		try {
			String fileName = file.getName();
			String longString = fileName.split("[.]")[0];
			return Long.valueOf(longString);
		} catch (Throwable t) {
			return null;
		}
	}
}