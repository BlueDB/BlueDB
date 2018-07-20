package io.bluedb.disk.recovery;

import java.io.File;
import io.bluedb.api.exceptions.BlueDbException;

public class TimeStampedFile implements Comparable<TimeStampedFile> {
	private final Long timestamp;
	private final File file;

	public TimeStampedFile (File file) throws BlueDbException {
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

	public static Long extractTimestamp(File file) throws BlueDbException {
		try {
			String fileName = file.getName();
			String longString = fileName.split("[.]")[0];
			return Long.valueOf(longString);
		} catch (Throwable t) {
			throw new BlueDbException("failed to parse timestamp in change filename " + file.getName(), t);
		}
	}
}