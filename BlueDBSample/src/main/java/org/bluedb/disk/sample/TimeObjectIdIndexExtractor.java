package org.bluedb.disk.sample;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.bluedb.api.index.UUIDIndexKeyExtractor;
import org.bluedb.disk.sample.model.time.TimeObject;

public class TimeObjectIdIndexExtractor implements UUIDIndexKeyExtractor<TimeObject> {
	private static final long serialVersionUID = 1L;

	@Override
	public List<UUID> extractUUIDsForIndex(TimeObject value) {
		return Arrays.asList(value.getId());
	}

}
