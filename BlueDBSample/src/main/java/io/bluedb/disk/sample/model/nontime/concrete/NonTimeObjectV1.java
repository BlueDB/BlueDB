package io.bluedb.disk.sample.model.nontime.concrete;

import java.util.UUID;

import io.bluedb.disk.sample.model.nontime.NonTimeObject;

public class NonTimeObjectV1 implements NonTimeObject {
	private static final long serialVersionUID = 1L;
	
	private UUID id;
	private String data;
	
	public NonTimeObjectV1(UUID id, String data) {
		this.id = id;
		this.data = data;
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
		return "";
	}

	@Override
	public void setNewData(String newData) {
		//Can't do this
	}
}
