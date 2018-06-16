package io.bluedb.disk.recovery;

import java.io.Serializable;
import org.nustaq.serialization.FSTConfiguration;
import io.bluedb.api.Updater;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.Segment;

public class PendingChange {

	private static FSTConfiguration serializer = FSTConfiguration.createDefaultConfiguration();

	final BlueKey key;
	final Serializable oldValue;
	final Serializable newValue;
	final long timeCreated;
	
	private PendingChange(BlueKey key, Serializable oldValue, Serializable newValue) {
		this.key = key;
		this.oldValue = oldValue;
		this.newValue = newValue;
		timeCreated = System.currentTimeMillis();
	}
	
	public static PendingChange createDelete(BlueKey key){
		return new PendingChange(key, null, null);
	}

	public static PendingChange createInsert(BlueKey key, Serializable newValue){
		return new PendingChange(key, null, newValue);
	}

	public static <X extends Serializable> PendingChange createUpdate(BlueKey key, X oldValue, Updater<X> updater){
		X newValue = clone(oldValue);
		updater.update(newValue);
		return new PendingChange(key, oldValue, newValue);
	}

	public void applyChange(Segment segment) throws BlueDbException {
		if (isInsert()) {
			// TODO check for already existing?  probably not, probably do that in the calling function
			segment.put(key, newValue);
		} else if (isDelete()) {
			segment.delete(key);
		} else if (isUpdate()) {
			segment.put(key, newValue);
		} else {
			throw new BlueDbException("maleformed PendingChange: " + this.toString());
		}
	}

	public BlueKey getKey() {
		return key;
	}
	public Object getOldValue() {
		return oldValue;
	}
	public Object getNewValue() {
		return newValue;
	}

	public long getTimeCreated() {
		return timeCreated;
	}

	public boolean isDelete() {
		return newValue == null;
	}

	public boolean isInsert() {
		return newValue != null && oldValue == null;
	}

	public boolean isUpdate() {
		return newValue != null && oldValue != null;
	}

	@Override
	public String toString() {
		return "<" + key +": " + String.valueOf(oldValue) + "=> " + String.valueOf(newValue) + ">";
	}

	@SuppressWarnings("unchecked")
	private static <X extends Serializable> X clone(X object) {
		return (X) serializer.asObject(serializer.asByteArray(object));
	}
}
