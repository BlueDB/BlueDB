package io.bluedb.disk.serialization;

import org.nustaq.serialization.FSTConfiguration;

import io.bluedb.api.keys.IntegerKey;
import io.bluedb.api.keys.LongKey;
import io.bluedb.api.keys.StringKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.recovery.PendingChange;

public class FstConfigurationFactory {
	public static FSTConfiguration createFstConfiguration(Class<?>...registeredSerializableClasses) {
		FSTConfiguration config = FSTConfiguration.createDefaultConfiguration();
		config.setShareReferences(true);
		
		config.registerClass(
				BlueEntity.class, 
				IntegerKey.class, 
				LongKey.class, 
				StringKey.class, 
				TimeKey.class, 
				TimeFrameKey.class, 
				PendingChange.class
		);
		
		if(registeredSerializableClasses != null && registeredSerializableClasses.length > 0) {
			config.registerClass(registeredSerializableClasses);
		}
		
		return config;
	}
}
