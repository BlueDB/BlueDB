 package io.bluedb.disk.serialization;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.nustaq.serialization.simpleapi.DefaultCoder;

import io.bluedb.api.keys.IntegerKey;
import io.bluedb.api.keys.LongKey;
import io.bluedb.api.keys.StringKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.recovery.PendingChange;

public class ThreadLocalFstSerializer extends ThreadLocal<DefaultCoder> implements BlueSerializer {
	
	private Class<?>[] registeredSerializableClasses;
	
	public ThreadLocalFstSerializer(Class<?>...registeredSerializableClasses) {
		this.registeredSerializableClasses = registeredSerializableClasses;
	}

	@Override
	protected DefaultCoder initialValue() {
		List<Class<?>> classesToRegister = new LinkedList<>();
		
		classesToRegister.addAll(getClassesToAlwaysRegister());
		classesToRegister.addAll(Arrays.asList(registeredSerializableClasses));
		
		Class<?>[] array = new Class<?>[0];
		return new DefaultCoder(true, classesToRegister.toArray(array));
	}

	private Collection<? extends Class<?>> getClassesToAlwaysRegister() {
		return Arrays.asList(
			BlueEntity.class, 
			IntegerKey.class, 
			LongKey.class, 
			StringKey.class, 
			TimeKey.class, 
			TimeFrameKey.class, 
			PendingChange.class
		);
	}

	@Override
	public byte[] serializeObjectToByteArray(Object o) {
		return get().toByteArray(o);
	}

	@Override
	public Object deserializeObjectFromByteArray(byte[] bytes) {
		return get().toObject(bytes);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Serializable> T clone(T object) {
		return (T) get().toObject(get().toByteArray(object));
	}
}
