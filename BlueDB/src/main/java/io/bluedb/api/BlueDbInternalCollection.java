package io.bluedb.api;

import java.io.Serializable;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;

public interface BlueDbInternalCollection<T extends Serializable> extends BlueDbCollection<T> {
	List<T> getAll(List<Condition<BlueKey>> keyConditions, List<Condition<T>> objectConditions) throws BlueDbException;

	void deleteAll(List<Condition<BlueKey>> keyConditions, List<Condition<T>> objectConditions) throws BlueDbException;

	void updateAll(List<Condition<BlueKey>> keyConditions, List<Condition<T>> objectConditions, Updater<T> updater) throws BlueDbException;
}
