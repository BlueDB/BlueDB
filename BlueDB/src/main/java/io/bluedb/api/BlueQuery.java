package io.bluedb.api;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;

public interface BlueQuery<T extends Serializable> {

	BlueQuery<T> where(Condition<T> c);

	BlueQuery<T> beforeTime(long time);
	BlueQuery<T> beforeOrAtTime(long time);

	BlueQuery<T> afterTime(long time);
	BlueQuery<T> afterOrAtTime(long time);

	List<T> getList() throws BlueDbException;
	Iterator<T> getIterator() throws BlueDbException;

	void delete() throws BlueDbException;

	void update(Updater<T> updater) throws BlueDbException;

}
