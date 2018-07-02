package io.bluedb.disk.serialization;

import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.io.Serializable;

import org.nustaq.serialization.FSTConfiguration;

public class FstSerializer implements BlueSerializer {
	
	private FSTConfiguration fstConfig;
	
	public FstSerializer(Class<?>...registeredSerializableClasses) {
		fstConfig = FstConfigurationFactory.createFstConfiguration(registeredSerializableClasses);
	}

	@Override
	public byte[] serializeObjectToByteArray(Object o) {
		return fstConfig.asByteArray(o);
	}

	@Override
	public Object deserializeObjectFromByteArray(byte[] bytes) {
		return fstConfig.asObject(bytes);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Serializable> T clone(T object) {
		return (T) fstConfig.asObject(fstConfig.asByteArray(object));
	}

	@Override
	public ObjectOutput getObjectOutputStream(OutputStream out) {
		return fstConfig.getObjectOutput(out);
	}

	@Override
	public ObjectInput getObjectInputStream(InputStream in) {
		return fstConfig.getObjectInput(in);
	}
}
