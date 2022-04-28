package org.bluedb.disk.time;

public class StandardTimeService implements TimeService {

	@Override
	public long getCurrentTime() {
		return System.currentTimeMillis();
	}

}
