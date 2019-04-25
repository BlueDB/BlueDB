 package org.bluedb.disk.serialization;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.nustaq.serialization.simpleapi.DefaultCoder;

import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.api.keys.UUIDKey;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.collection.index.IndexCompositeKey;
import org.bluedb.disk.recovery.PendingChange;
import org.bluedb.disk.serialization.validation.ObjectValidation;
import org.bluedb.disk.serialization.validation.SerializationException;

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
