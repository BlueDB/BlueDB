package org.bluedb.disk.sample.model.timeframe.concrete;

import java.util.UUID;

import org.bluedb.disk.sample.model.timeframe.TimeFrameObject;

public class TimeFrameObjectV1 implements TimeFrameObject {
	private static final long serialVersionUID = 1L;
	
	private UUID id;
	private long start;
	private long end;
	private String data;
	
	public TimeFrameObjectV1(UUID id, long start, long end, String data) {
		this.id = id;
		this.start = start;
		this.end = end;
		this.data = data;
	}

	@Override
	public UUID getId() {
		return id;
	}

	@Override
	public long getStart() {
		return start;
	}

	@Override
	public void setStart(long start) {
		this.start = start;
	}

	@Override
	public long getEnd() {
		return end;
	}

	@Override
	public void setEnd(long end) {
		this.end = end;
	}

	@Override
	public String getData() {
		return data;
	}

	@Override
	public void setData(String data) {
		this.data = data;
	}
}
