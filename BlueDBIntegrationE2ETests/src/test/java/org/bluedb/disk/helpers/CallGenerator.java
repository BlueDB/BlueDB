package org.bluedb.disk.helpers;

import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.disk.models.calls.Call;
import org.bluedb.disk.serialization.BlueEntity;

public class CallGenerator {
	public static BlueEntity<Call> generateBasicTestCallEntity() {
		return wrapCallAsEntity(Call.generateBasicTestCall());
	}
	
	public static BlueEntity<Call> generateBasicTestCallEntity(long start) {
		return wrapCallAsEntity(Call.generateBasicTestCall(start));
	}
	
	public static BlueEntity<Call> wrapCallAsEntity(Call call) {
		return new BlueEntity<Call>(new TimeFrameKey(call.getId(), call.getStart(), call.getEnd()), call);
	}
}
