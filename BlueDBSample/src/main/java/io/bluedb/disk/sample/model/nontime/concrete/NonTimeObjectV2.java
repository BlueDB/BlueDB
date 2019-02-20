package io.bluedb.disk.sample.model.nontime.concrete;

import java.util.UUID;

import io.bluedb.disk.sample.model.nontime.NonTimeObject;

public class NonTimeObjectV2 implements NonTimeObject {
	private static final long serialVersionUID = 1L;
	
	private UUID id;
	private String data;
	private String newData;
	
	public NonTimeObjectV2(UUID id, String data, String newData) {
		this.id = id;
		this.data = data;
		this.newData = newData;
	}

	@Override
	public UUID getId() {
		return id;
	}

	@Override
	public String getData() {
		return data;
	}

	@Override
	public void setData(String data) {
		this.data = data;
	}

	@Override
	public String getNewData() {
		return newData;
	}

	@Override
	public void setNewData(String newData) {
		this.newData = newData;
	}
}
