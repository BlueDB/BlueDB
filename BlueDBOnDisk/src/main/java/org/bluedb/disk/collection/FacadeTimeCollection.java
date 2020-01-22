package org.bluedb.disk.collection;

import java.io.Serializable;

import org.bluedb.api.ReadBlueTimeQuery;
import org.bluedb.api.ReadableBlueTimeCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.ReadableBlueDbOnDisk;

public class FacadeTimeCollection<T extends Serializable> extends FacadeCollection<T> implements ReadableBlueTimeCollection<T> {

	public FacadeTimeCollection(ReadableBlueDbOnDisk db, String name, Class<T> valueType) {
		super(db, name, valueType);
	}

	@Override
	protected ReadableBlueTimeCollection<T> getCollection() {
		if (db.collectionFolderExists(name)) {
			try {
				return db.getTimeCollection(name, valueType);
			} catch (BlueDbException e) {
				return dummyCollection;
			}
		} else {
			return dummyCollection;
		}
	}

	@Override
	public ReadBlueTimeQuery<T> query() {
		return getCollection().query();
	}
}
