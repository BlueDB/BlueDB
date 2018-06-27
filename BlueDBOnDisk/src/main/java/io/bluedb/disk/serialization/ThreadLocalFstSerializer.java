package io.bluedb.disk.serialization;

import java.io.Serializable;

import org.nustaq.serialization.FSTConfiguration;

public class ThreadLocalFstSerializer extends ThreadLocal<FSTConfiguration> implements BlueSerializer {
	
	private Class<?>[] registeredSerializableClasses;
	
	public ThreadLocalFstSerializer(Class<?>...registeredSerializableClasses) {
		this.registeredSerializableClasses = registeredSerializableClasses;
	}

	@Override
	protected FSTConfiguration initialValue() {
		return FstConfigurationFactory.createFstConfiguration(registeredSerializableClasses);
	}

	@Override
	public byte[] serializeObjectToByteArray(Object o) {
		return get().asByteArray(o);
	}

	@Override
	public Object deserializeObjectFromByteArray(byte[] bytes) {
		return get().asObject(bytes);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Serializable> T clone(T object) {
		return (T) get().asObject(get().asByteArray(object));
	}
}
