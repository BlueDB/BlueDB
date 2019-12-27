package org.bluedb.disk.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.bluedb.TestUtils;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.TestValue2;
import org.bluedb.disk.models.calls.Call;
import org.bluedb.disk.models.calls.CallEvent;
import org.bluedb.disk.models.calls.CallRecording;
import org.bluedb.disk.models.calls.CallV2;
import org.bluedb.disk.models.calls.Note;
import org.bluedb.disk.models.calls.RecordingStatus;
import org.bluedb.disk.models.calls.Timeframe;
import org.bluedb.disk.serialization.validation.ObjectValidation;
import org.bluedb.disk.serialization.validation.SerializationException;
import org.junit.Test;

public class ThreadLocalFstSerializerTest {
	
	@Test
	public void testStaticRegisteredClassesProblem() {
		ThreadLocalFstSerializer s1 = new ThreadLocalFstSerializer(TestValue.class);
		TestValue value1 = new TestValue("Derek", 1);
		TestValue clone1 = s1.clone(value1);
		assertEquals(value1, clone1);
		
		ThreadLocalFstSerializer s2 = new ThreadLocalFstSerializer(TestValue2.class);
		TestValue value2 = new TestValue("Derek2", 2);
		TestValue clone2 = s2.clone(value2);
		assertEquals(value2, clone2);
		
		TestValue value3 = new TestValue("Derek3", 3);
		TestValue clone3 = s1.clone(value3);
		assertEquals(value3, clone3);
	}
	
	@Test
	public void testAddingRegisteredClass() throws SerializationException {
		ThreadLocalFstSerializer s1 = new ThreadLocalFstSerializer();
		ThreadLocalFstSerializer s2 = new ThreadLocalFstSerializer(TestValue.class);
		
		TestValue originalValue = new TestValue("Derek", 3);
		byte[] bytes = s1.serializeObjectToByteArray(originalValue);
		TestValue newValue = (TestValue) s2.deserializeObjectFromByteArray(bytes);
		
		assertEquals(originalValue, newValue);
	}
	
	@Test
	public void testSerializingInvalidObject() throws URISyntaxException, BlueDbException, IOException {
		ThreadLocalFstSerializer serializer = new ThreadLocalFstSerializer(Call.getClassesToRegister());
		Path invalidObjectPath = TestUtils.getResourcePath("corruptCall-1.bin");
		Object invalidObject = serializer.deserializeObjectFromByteArrayWithoutChecks(Files.readAllBytes(invalidObjectPath));
		
		try {
			serializer.validateObjectBeforeSerializing(invalidObject);
			fail(); //It should have thrown an exception
		} catch(SerializationException e) {
		}
		
		try {
			serializer.serializeValidObject(invalidObject);
			fail(); //It should have thrown an exception
		} catch(SerializationException e) {
		}
		
		try {
			serializer.serializeObjectToByteArray(invalidObject);
			fail(); //It should have thrown an exception
		} catch(SerializationException e) {
		}
		
		byte[] invalidBytes = serializer.serializeObjectToByteArrayWithoutChecks(invalidObject);
		try {
			serializer.validateBytesAfterSerialization(invalidBytes);
			fail(); //It should have thrown an exception
		} catch(SerializationException e) {
		}
	}
	
	@Test
	public void testDeserializationIssue() {
		int testCount = 200;
		int mutationCountPerTest = 15;
		int callCountPerTest = 100;
		Random random = new Random();
		
		ThreadLocalFstSerializer serializer = new ThreadLocalFstSerializer(Call.getClassesToRegister());
		
		for(int i = 0; i < testCount; i++) {
			try {
				testSerializationAndMutationOfTestCalls(mutationCountPerTest, callCountPerTest, serializer, random);
			} catch (IllegalArgumentException | IllegalAccessException | SerializationException e) {
				e.printStackTrace();
				fail();
			}
		}
	}

	private void testSerializationAndMutationOfTestCalls(int mutationCountPerTest, int callCountPerTest, ThreadLocalFstSerializer serializer, Random random) throws IllegalArgumentException, IllegalAccessException, SerializationException {
		List<byte[]> currentCallsAsBytes = generateTestCallsAsBytes(callCountPerTest, serializer);
		List<byte[]> mutatedCallsAsBytes = new ArrayList<>();
		
		for(int i = 0; i < mutationCountPerTest; i++) {
			for(byte[] callBytes : currentCallsAsBytes) {
				Object originalObject = serializer.deserializeObjectFromByteArray(callBytes);
				
				ObjectValidation.validateFieldValueTypesForObject(originalObject);
				
				mutateCall((Call) ((BlueEntity<?>) originalObject).getValue(), random);
				
				byte[] mutatedBytes = serializer.serializeObjectToByteArrayWithoutChecks(originalObject);
				
				mutatedCallsAsBytes.add(mutatedBytes);
			}
			
			currentCallsAsBytes = mutatedCallsAsBytes;
			mutatedCallsAsBytes = new LinkedList<>();
		}
	}
	
	private List<byte[]> generateTestCallsAsBytes(int callCountPerTest, ThreadLocalFstSerializer serializer) throws SerializationException {
		List<byte[]> testCallsAsBytes = new LinkedList<>();
		for(int i = 0; i < callCountPerTest; i++) {
			Call call = CallV2.generateBasicTestCall();
			BlueEntity<Call> entity = new BlueEntity<Call>(new TimeFrameKey(call.getId(), call.getStart(), call.getEnd()), call);
			testCallsAsBytes.add(serializer.serializeObjectToByteArrayWithoutChecks(entity));
		}
		return testCallsAsBytes;
	}

	private void mutateCall(Call call, Random random) {
		switch(random.nextInt(4)) {
		case 0:
			addNote(call, random);
			break;
		case 1:
			addRecording(call, random);
			break;
		case 2:
			addAccountCode(call, random);
			break;
		case 3:
			setRecordingAsSaved(call);
			break;
		}
	}

	private void addNote(Call call, Random random) {
		Note note = new Note(UUID.randomUUID(), call.getStart(), "Derek Johnson(210)", "This is a rad note!", System.currentTimeMillis());
		call.addNote(note);
	}

	private void addRecording(Call call, Random random) {
		CallEvent firstEvent = call.getEvents().get(0);
		
		List<Timeframe> times = new ArrayList<>();
		times.add(new Timeframe(firstEvent.getStart(), firstEvent.getEnd()));
		
		CallRecording recording = new CallRecording(UUID.randomUUID(), call.getId(), firstEvent.getEventId(), "Stream " + random.nextInt(), RecordingStatus.PENDING, "SIP-ID" + random.nextInt(), call.getStart(), call.getEnd() - call.getStart(), UUID.randomUUID(), 1_000_000, times);
		call.addRecording(firstEvent.getEventId(), recording);
	}

	private void addAccountCode(Call call, Random random) {
		List<String> accountCodes = call.getAccountCodes();
		if(accountCodes == null) {
			accountCodes = new ArrayList<>();
		}
		accountCodes.add("My new account code" + random.nextInt(1000));
		call.setAccountCodes(accountCodes);
	}

	private void setRecordingAsSaved(Call call) {
		long eventId = call.getEvents().get(0).getEventId();
		call.setRecordingsOnEventAsSaved(eventId);
	}
}
