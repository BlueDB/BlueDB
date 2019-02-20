package io.bluedb.disk.sample.model.time;

import java.io.Serializable;
import java.util.UUID;

import io.bluedb.api.keys.TimeKey;

public interface TimeObject extends Serializable {
	public UUID getId();

	public long getStart();
	public void setStart(long start);

	public String getData();
	public void setData(String data);
	
	public default TimeKey getKey() {
		return new TimeKey(getId(), getStart());
	}
}
