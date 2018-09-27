package io.bluedb.disk.recovery;

import java.io.File;
import io.bluedb.api.exceptions.BlueDbException;

public class TimeStampedFile implements Comparable<TimeStampedFile> {
	private final Long timestamp;
	private final Long recoverableId;
	private final File file;

	public TimeStampedFile (File file) throws BlueDbException {
		this.file = file;
		this.timestamp = extractTimestamp(file);
		this.recoverableId = extractRecoverableId(file);
	}

	public File getFile() {
		return file;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public Long getRecoverableId() {
		return recoverableId;
	}

	@Override
	public int compareTo(TimeStampedFile o) {
		if (timestamp.compareTo(o.timestamp) == 0) {
			return recoverableId.compareTo(o.getRecoverableId());
		}
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

	public static Long extractRecoverableId(File file) {
		try {
			String fileName = file.getName();
			String longString = fileName.split("[.]")[1];
			return Long.valueOf(longString);
		} catch (Throwable t) {
			return 0L;
		}
	}
}