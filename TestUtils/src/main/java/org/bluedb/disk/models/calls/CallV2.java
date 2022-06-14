package org.bluedb.disk.models.calls;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class CallV2 extends Call {
	private static final long serialVersionUID = 1L;
	
	private String subClassField;
	
	public CallV2(UUID id, CallDirection callDirection, String callerId, String callingParty, String receivingParty, String group, long start, long end, 
			String tag, List<String> accountCodes, List<Note> notes, List<CallEvent> events, String subClassField) {
		super(id, callDirection, callerId, callingParty, receivingParty, group, start, end, tag, accountCodes, notes, events);
		this.subClassField = subClassField;
	}

	public String getSubClassField() {
		return subClassField;
	}
	
	public static CallV2 generateBasicTestCall() {
		Random r = new Random();
		
		UUID callId = UUID.randomUUID();
		CallDirection callDirection = CallDirection.OUTBOUND;
		String group = "";
		String tag = "";
		
		long callStart = r.nextInt(1_000_000);
		long ringStart = callStart + r.nextInt(30_000);
		long talkStart = ringStart + r.nextInt(30_000);
		long callEnd = talkStart + r.nextInt(30_000);
		
		String callingParty = "Calling Party " + r.nextInt(1000);
		String receivingParty = "Receiving Party " + r.nextInt(1000);
		String callerId = "Caller Id " + r.nextInt(1000);
		
		TrunkChannel trunkChannel = new TrunkChannel(r.nextInt(99), r.nextInt(24));
		List<TrunkChannel> trunkChannels = new ArrayList<>();
		trunkChannels.add(trunkChannel);
		
		List<String> accountCodes = null;
		List<Note> notes = null;
		List<CallRecording> recordings = null;
		
		List<CallEvent> events = new ArrayList<>();
		events.add(new CallEvent(r.nextLong(), CallEventType.DIALING, callingParty, receivingParty, group, callStart, ringStart, tag, trunkChannels, recordings));
		events.add(new CallEvent(r.nextLong(), CallEventType.RINGING, callingParty, receivingParty, group, ringStart, talkStart, tag, trunkChannels, recordings));
		events.add(new CallEvent(r.nextLong(), CallEventType.TALKING, callingParty, receivingParty, group, talkStart, callEnd, tag, trunkChannels, recordings));
		events.add(new CallEvent(r.nextLong(), CallEventType.CALLING_DROP, callingParty, receivingParty, group, callEnd, callEnd, tag, trunkChannels, recordings));
		
		CallV2 call = new CallV2(callId, callDirection, callerId, callingParty, receivingParty, group, callStart, callEnd, tag, accountCodes, notes, events, ""+r.nextInt());
		return call;
	}
}
