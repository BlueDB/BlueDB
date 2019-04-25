package org.bluedb.disk.serialization;

import java.io.Serializable;

import org.bluedb.disk.serialization.validation.SerializationException;

public interface BlueSerializer {

	public byte[] serializeObjectToByteArray(Object o);

	public Object deserializeObjectFromByteArray(byte[] bytes) throws SerializationException;

	public <T extends Serializable> T clone(T object);
}
