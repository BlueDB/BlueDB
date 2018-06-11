package io.bluedb.api.entities;

import io.bluedb.api.keys.TimeKey;

public interface BlueTimeEntity extends BlueEntity {
	@Override
	public TimeKey getKey();
}
