 package io.bluedb.disk.serialization;

import org.nustaq.serialization.simpleapi.DefaultCoder;

public class ThreadLocalFstReadSerializer extends ThreadLocal<DefaultCoder> {
	
	private Class<?>[] registeredSerializableClasses;
	
	public ThreadLocalFstReadSerializer(Class<?>...registeredSerializableClasses) {
		this.registeredSerializableClasses = registeredSerializableClasses;
	}

	@Override
	protected DefaultCoder initialValue() {
		return new DefaultCoder(true, registeredSerializableClasses);
	}

	public Object deserializeObjectFromByteArray(byte[] bytes) {
		return get().toObject(bytes);
	}
}
