package io.bluedb.api.entities;

import java.io.Serializable;
import io.bluedb.api.keys.BlueKey;

public interface BlueEntity extends Serializable {
	public BlueKey getKey();
}
