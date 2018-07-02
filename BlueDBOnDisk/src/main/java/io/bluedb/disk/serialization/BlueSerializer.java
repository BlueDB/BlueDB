package io.bluedb.disk.serialization;

import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.io.Serializable;

public interface BlueSerializer {

	public byte[] serializeObjectToByteArray(Object o);

	public Object deserializeObjectFromByteArray(byte[] bytes);

	public <T extends Serializable> T clone(T object);

	public ObjectOutput getObjectOutputStream(OutputStream out);

	public ObjectInput getObjectInputStream(InputStream in);
}
