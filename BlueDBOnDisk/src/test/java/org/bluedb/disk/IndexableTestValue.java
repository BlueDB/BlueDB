package org.bluedb.disk;

import java.io.Serializable;
import java.util.UUID;

import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.api.keys.UUIDKey;

public class IndexableTestValue implements Serializable {
	private static final long serialVersionUID = 1L;

	private UUID id;
	private long start;
	private long end;
	private String stringValue;
	private int intValue;
	
	public IndexableTestValue(UUID id, long start, long end, String stringValue, int intValue) {
		this.id = id;
		this.start = start;
		this.end = end;
		this.stringValue = stringValue;
		this.intValue = intValue;
	}

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public long getStart() {
		return start;
	}
	
	public void setStart(long start) {
		this.start = start;
	}
	
	public long getEnd() {
		return end;
	}
	
	public void setEnd(long end) {
		this.end = end;
	}

	public String getStringValue() {
		return stringValue;
	}

	public void setStringValue(String stringValue) {
		this.stringValue = stringValue;
	}

	public int getIntValue() {
		return intValue;
	}

	public void setIntValue(int intValue) {
		this.intValue = intValue;
	}
	
	public long getLongValue() {
		return start;
	}
	
	public TimeFrameKey getTimeFrameKey() {
		return new TimeFrameKey(id, start, end);
	}
	
	public TimeKey getTimeKey() {
		return new TimeKey(id, start);
	}
	
	public IntegerKey getIntegerKey() {
		return new IntegerKey(intValue);
	}
	
	public LongKey getLongKey() {
		return new LongKey(start);
	}
	
	public StringKey getStringKey() {
		return new StringKey(stringValue);
	}
	
	public UUIDKey getUUIDKey() {
		return new UUIDKey(id);
	}

	@Override
	public String toString() {
		return "IndexableTestValue [id=" + id + ", start=" + start + ", end=" + end + ", stringValue=" + stringValue
				+ ", intValue=" + intValue + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (end ^ (end >>> 32));
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + intValue;
		result = prime * result + (int) (start ^ (start >>> 32));
		result = prime * result + ((stringValue == null) ? 0 : stringValue.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IndexableTestValue other = (IndexableTestValue) obj;
		if (end != other.end)
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (intValue != other.intValue)
			return false;
		if (start != other.start)
			return false;
		if (stringValue == null) {
			if (other.stringValue != null)
				return false;
		} else if (!stringValue.equals(other.stringValue))
			return false;
		return true;
	}
}
