package org.bluedb.disk.sample.model.time.concrete;

import java.util.UUID;

import org.bluedb.disk.sample.model.time.TimeObject;

public class TimeObjectV1 implements TimeObject {
	private static final long serialVersionUID = 1L;
	
	private UUID id;
	private long start;
	private String data;
	
	public TimeObjectV1(UUID id, long start, String data) {
		this.id = id;
		this.start = start;
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
	public String getData() {
		return data;
	}

	@Override
	public void setData(String data) {
		this.data = data;
	}
}
