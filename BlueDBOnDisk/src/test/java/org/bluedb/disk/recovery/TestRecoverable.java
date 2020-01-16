package org.bluedb.disk.recovery;

import java.io.Serializable;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.ReadableBlueCollectionOnDisk;

public class TestRecoverable implements Recoverable<TestValue>, Serializable {

	private static final long serialVersionUID = 1L;

	private long time;
	private long recoverableId;

	public TestRecoverable(long timeCreated) {
		this.time = timeCreated;
	}

	@Override
	public void apply(ReadableBlueCollectionOnDisk<TestValue> collection) throws BlueDbException {}

	@Override
	public long getTimeCreated() {
		return time;
	}


	@Override
	public long getRecoverableId() {
		return recoverableId;
	}

	@Override
	public void setRecoverableId(long recoverableId) {
		this.recoverableId = recoverableId;
	}
}
