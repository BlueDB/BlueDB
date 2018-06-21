package io.bluedb.disk.serialization;

import java.io.Serializable;

public interface BlueSerializer {

	public byte[] serializeObjectToByteArray(Object o);

	public Object deserializeObjectFromByteArray(byte[] bytes);

	public <T extends Serializable> T clone(T object);
}
