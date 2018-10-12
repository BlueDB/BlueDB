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

public class ThreadLocalFstSerializer implements BlueSerializer {
	
	private ThreadLocalFstWriteSerializer writeSerializer;
	private ThreadLocalFstReadSerializer readSerializer;
	
	public ThreadLocalFstSerializer(Class<?>...registeredSerializableClasses) {
		writeSerializer = new ThreadLocalFstWriteSerializer(registeredSerializableClasses);
		readSerializer = new ThreadLocalFstReadSerializer(registeredSerializableClasses);
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
		return writeSerializer.serializeObjectToByteArray(o);
	}

	@Override
	public Object deserializeObjectFromByteArray(byte[] bytes) {
		return readSerializer.deserializeObjectFromByteArray(bytes);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Serializable> T clone(T object) {
		return (T) readSerializer.deserializeObjectFromByteArray(writeSerializer.serializeObjectToByteArray(object));
	}
}
