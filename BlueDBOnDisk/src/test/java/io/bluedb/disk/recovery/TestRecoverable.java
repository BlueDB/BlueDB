package io.bluedb.disk.recovery;

import java.io.Serializable;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.collection.BlueCollectionOnDisk;

public class TestRecoverable implements Recoverable<TestValue>, Serializable {

	private static final long serialVersionUID = 1L;

	private long time;
	private long recoverableId;

	public TestRecoverable(long timeCreated) {
		this.time = timeCreated;
	}

	@Override
	public void apply(BlueCollectionOnDisk<TestValue> collection) throws BlueDbException {}

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
