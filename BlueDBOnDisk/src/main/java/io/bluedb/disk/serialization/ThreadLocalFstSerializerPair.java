 package io.bluedb.disk.serialization;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import io.bluedb.api.keys.IntegerKey;
import io.bluedb.api.keys.LongKey;
import io.bluedb.api.keys.StringKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.api.keys.UUIDKey;
import io.bluedb.disk.collection.index.IndexCompositeKey;
import io.bluedb.disk.recovery.PendingChange;

public class ThreadLocalFstSerializerPair implements BlueSerializer {
	
	private ThreadLocalFstSerializer writeSerializer;
	private ThreadLocalFstSerializer readSerializer;
	
	public ThreadLocalFstSerializerPair(Class<?>...registeredSerializableClasses) {
		writeSerializer = new ThreadLocalFstSerializer(registeredSerializableClasses);
		readSerializer = new ThreadLocalFstSerializer(registeredSerializableClasses);
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
		return writeSerializer.get().toByteArray(o);
	}

	@Override
	public Object deserializeObjectFromByteArray(byte[] bytes) {
		return readSerializer.get().toObject(bytes);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Serializable> T clone(T object) {
		return (T) readSerializer.get().toObject(writeSerializer.get().toByteArray(object));
	}
}
