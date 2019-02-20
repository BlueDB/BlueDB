package io.bluedb.disk.sample.model.nontime;

import java.io.Serializable;
import java.util.UUID;

import io.bluedb.api.keys.UUIDKey;

public interface NonTimeObject extends Serializable {
	public UUID getId();
	
	public String getData();
	public void setData(String data);
	
	public String getNewData();
	public void setNewData(String newData);
	
	public default UUIDKey getKey() {
		return new UUIDKey(getId());
	}
}
