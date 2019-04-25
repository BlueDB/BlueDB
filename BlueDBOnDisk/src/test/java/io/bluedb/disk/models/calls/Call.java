package io.bluedb.disk.models.calls;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class Call implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private UUID id;
	private CallDirection callDirection;
	private String callerId;
	private String callingParty;
	private String receivingParty;
	private String group;
	private long start;
	private long end;
	
	private String tag;
	
	private List<String> accountCodes;
	private List<Note> notes;
	
	private List<CallEvent> events;

	public Call(UUID id, CallDirection callDirection, String callerId, String callingParty, String receivingParty,
			String group, long start, long end, String tag, List<String> accountCodes, List<Note> notes, List<CallEvent> events) {
		this.id = id;
		this.callDirection = callDirection;
		this.callerId = callerId;
		this.callingParty = callingParty;
		this.receivingParty = receivingParty;
		this.group = group;
		this.start = start;
		this.end = end;
		this.tag = tag;
		this.accountCodes = accountCodes;
		this.notes = notes;
		this.events = events;
	}

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public CallDirection getCallDirection() {
		return callDirection;
	}

	public void setCallDirection(CallDirection callDirection) {
		this.callDirection = callDirection;
	}

	public String getCallerId() {
		return callerId;
	}

	public void setCallerId(String callerId) {
		this.callerId = callerId;
	}

	public String getCallingParty() {
		return callingParty;
	}

	public void setCallingParty(String callingParty) {
		this.callingParty = callingParty;
	}

	public String getReceivingParty() {
		return receivingParty;
	}

	public void setReceivingParty(String receivingParty) {
		this.receivingParty = receivingParty;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}
	
	public String getTag() {
		return tag;
	}
	
	public void setTag(String tag) {
		this.tag = tag;
	}

	public List<String> getAccountCodes() {
		return accountCodes;
	}

	public void setAccountCodes(List<String> accountCodes) {
		this.accountCodes = accountCodes;
	}

	public List<Note> getNotes() {
		return notes;
	}

	public void setNotes(List<Note> notes) {
		this.notes = notes;
	}

	public List<CallEvent> getEvents() {
		return events;
	}

	public void setEvents(List<CallEvent> events) {
		this.events = events;
	}


	
	public static Call generateBasicTestCall() {
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
		
		Call call = new Call(callId, callDirection, callerId, callingParty, receivingParty, group, callStart, callEnd, tag, accountCodes, notes, events);
		return call;
	}

	public static Class<?>[] getClassesToRegister() {
		return new Class<?>[] {
			UUID.class,
			CallDirection.class,
			Note.class,
			CallEventType.class,
			TrunkChannel.class,
			CallRecording.class,
			RecordingStatus.class,
		};
	}

	public void addNote(Note note) {
		if(notes == null) {
			notes = new ArrayList<>();
		}
		notes.add(note);
	}

	public void addAccountCode(String string) {
		if(accountCodes == null) {
			accountCodes = new ArrayList<>();
		}
		accountCodes.add(string);
	}

	public void addRecording(long eventId, CallRecording recording) {
		for(CallEvent event : events) {
			if(event.getEventId() == eventId) {
				event.addRecording(recording);
				return;
			}
		}
	}

	public void setRecordingsOnEventAsSaved(long eventId) {
		for(CallEvent event : events) {
			if(event.getEventId() == eventId) {
				event.setRecordingsAsSaved();
				return;
			}
		}
	}
}
