package io.bluedb.disk.sample;

import java.util.Arrays;
import java.util.List;

import io.bluedb.api.index.KeyExtractor;
import io.bluedb.api.keys.StringKey;
import io.bluedb.disk.sample.model.time.TimeObject;

public class TimeObjectDataIndexExtractor implements KeyExtractor<StringKey, TimeObject> {
	private static final long serialVersionUID = 1L;

	@Override
	public List<StringKey> extractKeys(TimeObject object) {
		return Arrays.asList(new StringKey(object.getData()));
	}

	@Override
	public Class<StringKey> getType() {
		return StringKey.class;
	}
}
