package org.bluedb.disk.segment.writer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyObject;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.bluedb.TestUtils;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.disk.file.BlueObjectInput;
import org.bluedb.disk.file.BlueObjectOutput;
import org.bluedb.disk.metadata.BlueFileMetadata;
import org.bluedb.disk.metadata.BlueFileMetadataKey;
import org.bluedb.disk.models.calls.Call;
import org.bluedb.disk.recovery.InMemorySortedChangeSupplier;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.SortedChangeSupplier;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;
import org.bluedb.disk.serialization.validation.ObjectValidation;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class BatchWriterTest {
	
	private static BlueKey key1 = new LongKey(1);
	private static BlueKey key2 = new LongKey(2);
	private static BlueKey key3 = new LongKey(3);
	private static BlueKey key5 = new LongKey(5);
	private static BlueKey key7 = new LongKey(7);
	private static BlueKey key8 = new LongKey(8);
	private static BlueEntity<String> value1at1 = new BlueEntity<>(key1, "1");
	private static BlueEntity<String> value3at3 = new BlueEntity<>(key3, "3");
	private static BlueEntity<String> value5at5 = new BlueEntity<>(key5, "5");
	private static BlueEntity<String> value7at7 = new BlueEntity<>(key7, "7");
	private static BlueEntity<String> value5bAt5 = new BlueEntity<>(key5, "5b");
	private static IndividualChange<String> delete2 = new IndividualChange<>(key2, null, null);
	private static IndividualChange<String> delete3 = new IndividualChange<>(key3, null, null);
	private static IndividualChange<String> delete5 = new IndividualChange<>(key5, null, null);
	private static IndividualChange<String> delete8 = new IndividualChange<>(key8, null, null);

	private static IndividualChange<String> insert1 = new IndividualChange<>(key1, null, "1");

	private static IndividualChange<String> update5bAt5 = new IndividualChange<>(key5, "5", "5b");
	
	private ThreadLocalFstSerializer serializer = new ThreadLocalFstSerializer(new Class<?>[] { });

	@Test
	public void testDeletes() throws Exception {
		List<BlueEntity<String>> initialValues = Arrays.asList(value3at3, value5at5, value7at7);
		BlueObjectInput<BlueEntity<String>> mockInput = createMockInput(serializer, initialValues);
		
		List<BlueEntity<String>> results = new ArrayList<>();
		BlueObjectOutput<BlueEntity<String>> mockOutput = createMockOutput(serializer, results);

		SortedChangeSupplier<String> sortedChangeSupplier = new InMemorySortedChangeSupplier<>(Arrays.asList(delete2, delete3, delete5, delete8));
		Range range = new Range(delete2.getGroupingNumber(), delete8.getGroupingNumber());
		sortedChangeSupplier.seekToNextChangeInRange(range);
		
		BatchWriter<String> batchDeletes2and3and5and8 = new BatchWriter<>(sortedChangeSupplier, range);
		batchDeletes2and3and5and8.process(mockInput, mockOutput);
		
		assertEquals(Arrays.asList(value7at7), results);
	}

	@Test
	public void testUpdateAndInsert() throws Exception {
		List<BlueEntity<String>> initialValues = Arrays.asList(value3at3, value5at5, value7at7);
		BlueObjectInput<BlueEntity<String>> mockInput = createMockInput(serializer, initialValues);
		
		List<BlueEntity<String>> results = new ArrayList<>();
		BlueObjectOutput<BlueEntity<String>> mockOutput = createMockOutput(serializer, results);

		SortedChangeSupplier<String> sortedChangeSupplier = new InMemorySortedChangeSupplier<>(Arrays.asList(insert1, update5bAt5));
		Range range = new Range(insert1.getGroupingNumber(), update5bAt5.getGroupingNumber());
		sortedChangeSupplier.seekToNextChangeInRange(range);
		
		BatchWriter<String> batchInsert1andUpdate5 = new BatchWriter<>(sortedChangeSupplier, range);
		batchInsert1andUpdate5.process(mockInput, mockOutput);
		
		assertEquals(Arrays.asList(value1at1, value3at3, value5bAt5, value7at7), results);
	}

	@Test
	public void test_UpdateAndInsert_changedMetadata() throws Exception {
		List<BlueEntity<String>> initialValues = Arrays.asList(value3at3, value5at5, value7at7);
		BlueObjectInput<BlueEntity<String>> mockInput = createMockInput(serializer, initialValues);
		

		List<BlueEntity<String>> results = new ArrayList<>();
		BlueObjectOutput<BlueEntity<String>> mockOutput = createMockOutput(serializer, results);
		BlueFileMetadata outputMetadata = new BlueFileMetadata();
		outputMetadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, "output-key");
		Mockito.when(mockOutput.getMetadata()).thenReturn(outputMetadata);

		SortedChangeSupplier<String> sortedChangeSupplier = new InMemorySortedChangeSupplier<>(Arrays.asList(insert1, update5bAt5));
		Range range = new Range(insert1.getGroupingNumber(), update5bAt5.getGroupingNumber());
		sortedChangeSupplier.seekToNextChangeInRange(range);
		
		BatchWriter<String> batchInsert1andUpdate5 = new BatchWriter<>(sortedChangeSupplier, range);
		batchInsert1andUpdate5.process(mockInput, mockOutput);

		assertEquals(Arrays.asList(value1at1, value3at3, value5bAt5, value7at7), results);
	}
	
	@Test
	public void testUpdatingInvalidObjectThenMakingOtherChanges() throws BlueDbException, IOException, URISyntaxException {
		serializer = new ThreadLocalFstSerializer(Call.getClassesToRegister());
		
		BlueEntity<Call> invalidCall = TestUtils.loadCorruptCall();
		long invalidCallStart = invalidCall.getValue().getStart();
		
		BlueEntity<Call> call1 = Call.generateBasicTestCallEntity(invalidCallStart - 10);
		BlueEntity<Call> call3 = Call.generateBasicTestCallEntity(invalidCallStart + 10);
		BlueEntity<Call> call4 = Call.generateBasicTestCallEntity(invalidCallStart + 20);
		BlueEntity<Call> call5 = Call.generateBasicTestCallEntity(invalidCallStart + 30);
		
		@SuppressWarnings("deprecation")
		BlueEntity<Call> updatedInvalidCall = serializer.cloneWithoutChecks(invalidCall);
		updatedInvalidCall.getValue().setReceivingParty("testChange");
		
		BlueEntity<Call> updatedcall4 = Call.wrapCallAsEntity(call4.getValue().clone());
		updatedcall4.getValue().setCallingParty("testChange");
		
		List<BlueEntity<Call>> initialValues = new LinkedList<>(Arrays.asList(call1, invalidCall, call4, call5));
		BlueObjectInput<BlueEntity<Call>> mockInput = createMockInput(serializer, initialValues);
		
		List<BlueEntity<Call>> results = new ArrayList<>();
		BlueObjectOutput<BlueEntity<Call>> mockOutput = createMockOutput(serializer, results);
		
		IndividualChange<Call> updateInvalidCall = new IndividualChange<Call>(invalidCall.getKey(), invalidCall.getValue(), updatedInvalidCall.getValue());
		IndividualChange<Call> insert3 = IndividualChange.createInsertChange(call3.getKey(), call3.getValue());
		IndividualChange<Call> update4 = new IndividualChange<Call>(call4.getKey(), call4.getValue(), updatedcall4.getValue());
		IndividualChange<Call> delete5 = IndividualChange.createDeleteChange(call5);
		
		SortedChangeSupplier<Call> sortedChangeSupplier = new InMemorySortedChangeSupplier<Call>(Arrays.asList(updateInvalidCall, insert3, update4, delete5));
		Range range = new Range(updateInvalidCall.getGroupingNumber(), delete5.getGroupingNumber());
		sortedChangeSupplier.seekToNextChangeInRange(range);
		
		BatchWriter<Call> batchWriter = new BatchWriter<Call>(sortedChangeSupplier, range);
		batchWriter.process(mockInput, mockOutput);
		
		BlueEntity<Call> resultingCall1 = results.stream().filter(callEntity -> callEntity.getKey().equals(call1.getKey())).findAny().orElse(null);
		BlueEntity<Call> resultingInvalidCall = results.stream().filter(callEntity -> callEntity.getKey().equals(invalidCall.getKey())).findAny().orElse(null);
		BlueEntity<Call> resultingCall3 = results.stream().filter(callEntity -> callEntity.getKey().equals(call3.getKey())).findAny().orElse(null);
		BlueEntity<Call> resultingCall4 = results.stream().filter(callEntity -> callEntity.getKey().equals(call4.getKey())).findAny().orElse(null);
		BlueEntity<Call> resultingCall5 = results.stream().filter(callEntity -> callEntity.getKey().equals(call5.getKey())).findAny().orElse(null);
		
		assertNotNull(resultingCall1); //This call was unchanged
		
		assertNotNull(resultingInvalidCall);
		assertNotEquals("testChange", resultingInvalidCall.getValue().getReceivingParty()); //Change won't be made because the new object failed to serialize properly
		
		assertNotNull(resultingCall3); //Call was successfully inserted
		
		assertNotNull(resultingCall4);
		assertEquals("testChange", resultingCall4.getValue().getCallingParty()); //Call was successfully updated
		
		assertNull(resultingCall5); //Call was successfully deleted
	}

	@Test
	public void testUpdatingInvalidObjectThenMakingOtherChanges_changedMetadata() throws BlueDbException, IOException, URISyntaxException {
		serializer = new ThreadLocalFstSerializer(Call.getClassesToRegister());

		BlueEntity<Call> invalidCall = TestUtils.loadCorruptCall();
		long invalidCallStart = invalidCall.getValue().getStart();

		BlueEntity<Call> call1 = Call.generateBasicTestCallEntity(invalidCallStart - 10);
		BlueEntity<Call> call3 = Call.generateBasicTestCallEntity(invalidCallStart + 10);
		BlueEntity<Call> call4 = Call.generateBasicTestCallEntity(invalidCallStart + 20);
		BlueEntity<Call> call5 = Call.generateBasicTestCallEntity(invalidCallStart + 30);

		@SuppressWarnings("deprecation")
		BlueEntity<Call> updatedInvalidCall = serializer.cloneWithoutChecks(invalidCall);
		updatedInvalidCall.getValue().setReceivingParty("testChange");

		BlueEntity<Call> updatedcall4 = Call.wrapCallAsEntity(call4.getValue().clone());
		updatedcall4.getValue().setCallingParty("testChange");

		List<BlueEntity<Call>> initialValues = new LinkedList<>(Arrays.asList(call1, invalidCall, call4, call5));
		BlueObjectInput<BlueEntity<Call>> mockInput = createMockInput(serializer, initialValues);

		List<BlueEntity<Call>> results = new ArrayList<>();
		BlueObjectOutput<BlueEntity<Call>> mockOutput = createMockOutput(serializer, results);
		BlueFileMetadata outputMetadata = new BlueFileMetadata();
		outputMetadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, "output-key");
		Mockito.when(mockOutput.getMetadata()).thenReturn(outputMetadata);

		IndividualChange<Call> updateInvalidCall = new IndividualChange<Call>(invalidCall.getKey(), invalidCall.getValue(), updatedInvalidCall.getValue());
		IndividualChange<Call> insert3 = IndividualChange.createInsertChange(call3.getKey(), call3.getValue());
		IndividualChange<Call> update4 = new IndividualChange<Call>(call4.getKey(), call4.getValue(), updatedcall4.getValue());
		IndividualChange<Call> delete5 = IndividualChange.createDeleteChange(call5);
		
		SortedChangeSupplier<Call> sortedChangeSupplier = new InMemorySortedChangeSupplier<Call>(Arrays.asList(updateInvalidCall, insert3, update4, delete5));
		Range range = new Range(updateInvalidCall.getGroupingNumber(), delete5.getGroupingNumber());
		sortedChangeSupplier.seekToNextChangeInRange(range);

		BatchWriter<Call> batchWriter = new BatchWriter<Call>(sortedChangeSupplier, range);
		batchWriter.process(mockInput, mockOutput);

		BlueEntity<Call> resultingCall1 = results.stream().filter(callEntity -> callEntity.getKey().equals(call1.getKey())).findAny().orElse(null);
		BlueEntity<Call> resultingInvalidCall = results.stream().filter(callEntity -> callEntity.getKey().equals(invalidCall.getKey())).findAny().orElse(null);
		BlueEntity<Call> resultingCall3 = results.stream().filter(callEntity -> callEntity.getKey().equals(call3.getKey())).findAny().orElse(null);
		BlueEntity<Call> resultingCall4 = results.stream().filter(callEntity -> callEntity.getKey().equals(call4.getKey())).findAny().orElse(null);
		BlueEntity<Call> resultingCall5 = results.stream().filter(callEntity -> callEntity.getKey().equals(call5.getKey())).findAny().orElse(null);

		assertNotNull(resultingCall1); //This call was unchanged

		assertNotNull(resultingInvalidCall);
		assertNotEquals("testChange", resultingInvalidCall.getValue().getReceivingParty()); //Change won't be made because the new object failed to serialize properly

		assertNotNull(resultingCall3); //Call was successfully inserted

		assertNotNull(resultingCall4);
		assertEquals("testChange", resultingCall4.getValue().getCallingParty()); //Call was successfully updated

		assertNull(resultingCall5); //Call was successfully deleted
	}
	
	@Test
	public void testDeletingInvalidObjectThenMakingOtherChanges() throws URISyntaxException, IOException, BlueDbException {
		serializer = new ThreadLocalFstSerializer(Call.getClassesToRegister());
		
		BlueEntity<Call> invalidCall = TestUtils.loadCorruptCall();
		long invalidCallStart = invalidCall.getValue().getStart();
		
		BlueEntity<Call> call2 = Call.generateBasicTestCallEntity(invalidCallStart + 20);
		
		List<BlueEntity<Call>> initialValues = new LinkedList<>(Arrays.asList(invalidCall, call2));
		BlueObjectInput<BlueEntity<Call>> mockInput = createMockInput(serializer, initialValues);
		
		List<BlueEntity<Call>> results = new ArrayList<>();
		BlueObjectOutput<BlueEntity<Call>> mockOutput = createMockOutput(serializer, results);
		
		BlueEntity<Call> updatedcall2 = Call.wrapCallAsEntity(call2.getValue().clone());
		updatedcall2.getValue().setCallingParty("testChange2");
		
		IndividualChange<Call> deleteInvalidObject = IndividualChange.createDeleteChange(invalidCall);
		IndividualChange<Call> update4Again = new IndividualChange<Call>(call2.getKey(), call2.getValue(), updatedcall2.getValue());
		
		SortedChangeSupplier<Call> sortedChangeSupplier = new InMemorySortedChangeSupplier<Call>(Arrays.asList(deleteInvalidObject, update4Again));
		Range range = new Range(deleteInvalidObject.getGroupingNumber(), update4Again.getGroupingNumber());
		sortedChangeSupplier.seekToNextChangeInRange(range);
		
		BatchWriter<Call> batchWriter = new BatchWriter<Call>(sortedChangeSupplier, range);
		batchWriter.process(mockInput, mockOutput);
		
		BlueEntity<Call> resultingInvalidCall = results.stream().filter(callEntity -> callEntity.getKey().equals(invalidCall.getKey())).findAny().orElse(null);
		BlueEntity<Call> resultingCall2 = results.stream().filter(callEntity -> callEntity.getKey().equals(call2.getKey())).findAny().orElse(null);
		
		assertNull(resultingInvalidCall); //Delete an invalid call should still work
		
		assertNotNull(resultingCall2);
		assertEquals("testChange2", resultingCall2.getValue().getCallingParty()); //Update should still happen
	}
	
	@Test
	public void testInsertingInvalidObjectThenMakingOtherUpdates() throws BlueDbException, IOException, URISyntaxException {
		serializer = new ThreadLocalFstSerializer(Call.getClassesToRegister());
		
		BlueEntity<Call> invalidCall = TestUtils.loadCorruptCall();
		long invalidCallStart = invalidCall.getValue().getStart();
		
		BlueEntity<Call> call2 = Call.generateBasicTestCallEntity(invalidCallStart + 20);
		
		List<BlueEntity<Call>> initialValues = new LinkedList<>(Arrays.asList(call2));
		BlueObjectInput<BlueEntity<Call>> mockInput = createMockInput(serializer, initialValues);
		
		List<BlueEntity<Call>> results = new ArrayList<>();
		BlueObjectOutput<BlueEntity<Call>> mockOutput = createMockOutput(serializer, results);
		
		BlueEntity<Call> updatedcall2 = Call.wrapCallAsEntity(call2.getValue().clone());
		updatedcall2.getValue().setCallingParty("testChange3");
		
		IndividualChange<Call> insertInvalidObject = IndividualChange.createInsertChange(invalidCall.getKey(), invalidCall.getValue());
		IndividualChange<Call> updateCall4AThirdTime = new IndividualChange<Call>(call2.getKey(), updatedcall2.getValue(), updatedcall2.getValue());
		
		SortedChangeSupplier<Call> sortedChangeSupplier = new InMemorySortedChangeSupplier<Call>(Arrays.asList(insertInvalidObject, updateCall4AThirdTime));
		Range range = new Range(insertInvalidObject.getGroupingNumber(), updateCall4AThirdTime.getGroupingNumber());
		sortedChangeSupplier.seekToNextChangeInRange(range);
		
		BatchWriter<Call> batchWriter = new BatchWriter<Call>(sortedChangeSupplier, range);
		batchWriter.process(mockInput, mockOutput);
		
		BlueEntity<Call> resultingInvalidCall = results.stream().filter(callEntity -> callEntity.getKey().equals(invalidCall.getKey())).findAny().orElse(null);
		BlueEntity<Call> resultingCall2 = results.stream().filter(callEntity -> callEntity.getKey().equals(call2.getKey())).findAny().orElse(null);
		
		assertNull(resultingInvalidCall); //Insert should fail since the object cannot be serialized correctly
		
		assertNotNull(resultingCall2);
		assertEquals("testChange3", resultingCall2.getValue().getCallingParty()); //Update should still happen
	}

	@SuppressWarnings("deprecation")
	private static <T extends Serializable> BlueObjectInput<T> createMockInput(ThreadLocalFstSerializer serializer, List<T> values) throws BlueDbException {
		final LinkedList<T> inputValues = new LinkedList<>(values);
		@SuppressWarnings("unchecked")
		BlueObjectInput<T> mockInput = Mockito.mock(BlueObjectInput.class);
		Mockito.doAnswer((x) -> !inputValues.isEmpty()).when(mockInput).hasNext();
		Mockito.doAnswer((x) -> inputValues.poll()).when(mockInput).next();
		Mockito.doAnswer((x) -> serializer.serializeObjectToByteArrayWithoutChecks(inputValues.poll())).when(mockInput).nextUnencryptedBytesWithoutDeserializing();
		Mockito.doAnswer((x) -> serializer.serializeObjectToByteArrayWithoutChecks(inputValues.poll())).when(mockInput).nextRawBytesWithoutDeserializing();
		Mockito.doAnswer((x) -> inputValues.peek()).when(mockInput).peek();
		Mockito.when(mockInput.getMetadata()).thenReturn(new BlueFileMetadata());
		return mockInput;
	}

	private static <T extends Serializable> BlueObjectOutput<T> createMockOutput(ThreadLocalFstSerializer serializer, List<T> results) throws BlueDbException {
		@SuppressWarnings("unchecked")
		BlueObjectOutput<T> mockOutput = Mockito.mock(BlueObjectOutput.class);
		
		Answer<T> writeObjectMethod = (InvocationOnMock invocation) -> {
			@SuppressWarnings("unchecked")
			T outputValue = (T) invocation.getArguments()[0];
			
			ObjectValidation.validateFieldValueTypesForObject(outputValue);
			
			results.add(outputValue);
			return null;
		};
		
		@SuppressWarnings({ "deprecation", "unchecked" })
		Answer<T> writeBytesMethod = (InvocationOnMock invocation) -> {
			byte[] outputValueBytes = (byte[]) invocation.getArguments()[0];
			results.add((T)serializer.deserializeObjectFromByteArrayWithoutChecks(outputValueBytes));
			return null;
		};
		
		Mockito.doAnswer(writeObjectMethod).when(mockOutput).write(anyObject());
		Mockito.doAnswer(writeBytesMethod).when(mockOutput).writeBytesAndAllowEncryption(anyObject());
		Mockito.doAnswer(writeBytesMethod).when(mockOutput).writeBytesAndForceSkipEncryption(anyObject());
		Mockito.when(mockOutput.getMetadata()).thenReturn(new BlueFileMetadata());
		return mockOutput;
	}
}
