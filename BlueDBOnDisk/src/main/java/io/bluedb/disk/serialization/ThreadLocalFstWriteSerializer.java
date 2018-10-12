 package io.bluedb.disk.serialization;

import org.nustaq.serialization.simpleapi.DefaultCoder;

public class ThreadLocalFstWriteSerializer extends ThreadLocal<DefaultCoder> {
	
	private Class<?>[] registeredSerializableClasses;
	
	public ThreadLocalFstWriteSerializer(Class<?>...registeredSerializableClasses) {
		this.registeredSerializableClasses = registeredSerializableClasses;
	}

	@Override
	protected DefaultCoder initialValue() {
		return new DefaultCoder(true, registeredSerializableClasses);
	}

	public byte[] serializeObjectToByteArray(Object o) {
		return get().toByteArray(o);
	}
}
