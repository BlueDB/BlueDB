package org.bluedb.disk.sample;

import java.util.Arrays;
import java.util.List;

import org.bluedb.api.index.LongIndexKeyExtractor;
import org.bluedb.disk.sample.model.time.TimeObject;

public class TimeObjectStartIndexExtractor implements LongIndexKeyExtractor<TimeObject> {
	private static final long serialVersionUID = 1L;

	@Override
	public List<Long> extractLongsForIndex(TimeObject value) {
		return Arrays.asList(value.getStart());
	}

}
