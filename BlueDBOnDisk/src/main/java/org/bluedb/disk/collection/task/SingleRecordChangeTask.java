package org.bluedb.disk.collection.task;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.exceptions.DuplicateKeyException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.KeyValueToChangeMapper;
import org.bluedb.disk.recovery.PendingMassChange;
import org.bluedb.disk.recovery.RecoveryManager;

public class SingleRecordChangeTask<T extends Serializable> extends QueryTask {
	private final ReadWriteCollectionOnDisk<T> collection;
	private final BlueKey key;
	private final SingleRecordChangeMode mode;
	private final KeyValueToChangeMapper<T> changeMapper;

	public SingleRecordChangeTask(String description, ReadWriteCollectionOnDisk<T> collection, BlueKey key, KeyValueToChangeMapper<T> changeMapper, SingleRecordChangeMode mode) {
		super(description);
		this.collection = collection;
		this.key = key;
		this.mode = mode;
		this.changeMapper = changeMapper;
	}

	@Override
	public void execute() throws BlueDbException {
		RecoveryManager<T> recoveryManager = collection.getRecoveryManager();
		
		T value = collection.get(key);
		if(mode == SingleRecordChangeMode.REQUIRE_DOES_NOT_ALREADY_EXIST && value != null) {
			throw new DuplicateKeyException("Value for key " + key + " already exists", key);
		} else if(mode == SingleRecordChangeMode.REQUIRE_ALREADY_EXISTS && value == null) {
			throw new NoSuchElementException("Cannot find object for key: " + key);
		}
		
		List<IndividualChange<T>> changeList = Arrays.asList(changeMapper.map(key, value));
		
		PendingMassChange<T> changeBatch = recoveryManager.saveMassChange(changeList.iterator());
		changeBatch.apply(collection);
		recoveryManager.markComplete(changeBatch);
	}

	@Override
	public String toString() {
		return "<" + getClass().getSimpleName() + " for key " + key + ">";
	}
	
	public static enum SingleRecordChangeMode {
		NO_REQUIREMENTS,
		REQUIRE_DOES_NOT_ALREADY_EXIST,
		REQUIRE_ALREADY_EXISTS,
	}
}
