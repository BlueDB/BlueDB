package io.bluedb.disk.sample.model.timeframe;

import java.io.Serializable;
import java.util.UUID;

import io.bluedb.api.keys.TimeFrameKey;

public interface TimeFrameObject extends Serializable {
	public UUID getId();

	public long getStart();
	public void setStart(long start);

	public long getEnd();
	public void setEnd(long end);
	
	public String getData();
	public void setData(String data);
	
	public default TimeFrameKey getKey() {
		return new TimeFrameKey(getId(), getStart(), getEnd());
	}
}
