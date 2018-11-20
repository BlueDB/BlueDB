 package io.bluedb.disk.serialization;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.nustaq.serialization.simpleapi.DefaultCoder;

import io.bluedb.api.keys.IntegerKey;
import io.bluedb.api.keys.LongKey;
import io.bluedb.api.keys.StringKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.api.keys.UUIDKey;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.collection.index.IndexCompositeKey;
import io.bluedb.disk.recovery.PendingChange;
import io.bluedb.disk.serialization.validation.ObjectValidation;
import io.bluedb.disk.serialization.validation.SerializationException;

public class ThreadLocalFstSerializer extends ThreadLocal<DefaultCoder> implements BlueSerializer {
	
	private static final int MAX_DESERIALIZE_ATTEMPTS = 5;
	
	private Class<?>[] registeredSerializableClasses;
	
	public ThreadLocalFstSerializer(Class<?>...registeredSerializableClasses) {
		this.registeredSerializableClasses = registeredSerializableClasses;
	}

	@Override
	protected DefaultCoder initialValue() {
		return new DefaultCoder(true, registeredSerializableClasses);
	}

	public static Collection<? extends Class<? extends Serializable>> getClassesToAlwaysRegister() {
		return Arrays.asList(
			BlueEntity.class, 
			IntegerKey.class, 
			LongKey.class, 
			StringKey.class,
			TimeKey.class, 
			TimeFrameKey.class, 
			PendingChange.class,
			UUID.class,
			UUIDKey.class,
			IndexCompositeKey.class
		);
	}

	@Override
	public byte[] serializeObjectToByteArray(Object o) {
		return get().toByteArray(o);
	}

	@Override
	public Object deserializeObjectFromByteArray(byte[] bytes) throws SerializationException {
		Throwable failureCause = null;
		
		int retryCount = 0;
		while(retryCount < MAX_DESERIALIZE_ATTEMPTS) {
			Object obj = get().toObject(bytes);
			
			try {
				ObjectValidation.validateFieldValueTypesForObject(obj);
				return obj;
			} catch(Throwable t) {
				failureCause = t;
				retryCount++;
			}
		}
		
		throw new SerializationException("Failed to deserialize object from bytes: " + Blutils.toHex(bytes), failureCause);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Serializable> T clone(T object) {
		return (T) get().toObject(get().toByteArray(object));
	}
}
