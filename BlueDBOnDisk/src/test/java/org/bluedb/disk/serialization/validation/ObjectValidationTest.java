package org.bluedb.disk.serialization.validation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.bluedb.TestUtils;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.config.TestValidationConfigurationService;
import org.bluedb.disk.models.Blob;
import org.bluedb.disk.models.calls.Call;
import org.bluedb.disk.models.calls.CallV2;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;
import org.bluedb.tasks.TestTask;
import org.junit.Assert;
import org.junit.Test;

public class ObjectValidationTest {
	
	@Test
	public void testNull_doesNotThrowException() throws SerializationException {
		ObjectValidation.validateFieldValueTypesForObject(null);
	}

	@Test
	public void testValidateFieldValueTypesForValidObjects() {
		Random r = new Random();
		
		int validInt = r.nextInt();
		Integer validIntBoxed = new Integer(r.nextInt());
		Integer validIntBoxedNull = null;
		long validLong = r.nextLong();
		Long validLongBoxed = new Long(r.nextLong());
		Long validLongBoxedNull = null;
		float validFloat = r.nextFloat();
		Float validFloatBoxed = new Float(r.nextFloat());
		Float validFloatBoxedNull = null;
		double validDouble = r.nextDouble();
		Double validDoubleBoxed = new Double(r.nextDouble());
		Double validDoubleBoxedNull = null;
		byte validByte = (byte) 0x5A;
		Byte validByteBoxed = new Byte((byte) 0x5A);
		Byte validByteBoxedNull = null;
		boolean validBoolean = r.nextBoolean();
		Boolean validBooleanBoxed = new Boolean(r.nextBoolean());
		Boolean validBooleanBoxedNull = null;
		String validString = "My String";
		String validStringNull = null;
		
		TestValue testValue1 = new TestValue("Derek", Integer.MAX_VALUE); 
		TestValue testValue2 = new TestValue("Jeremy", -1); 
		
		int[] validIntArrayNull = null;
		int[] validIntArrayEmpty = new int[] { };
		int[] validIntArray = new int[] { r.nextInt(), r.nextInt() };
		
		List<TestValue> validTestValuesListNull = null;
		List<TestValue> validTestValuesListEmpty = new ArrayList<>();
		List<TestValue> validTestValuesList = new ArrayList<>(Arrays.asList(testValue1, testValue2));
		List<TestValue> validTestValuesListWithNull = new ArrayList<>(Arrays.asList(testValue1, testValue2, null));
		
		TestValue[] validTestValuesArrayNull = null;
		TestValue[] validTestValuesArrayEmpty = new TestValue[] { };
		TestValue[] validTestValuesArray = new TestValue[] { testValue1, testValue2 };
		TestValue[] validTestValuesArrayWithNull = new TestValue[] { testValue1, testValue2, null };
		
		Map<String, TestValue> validTestValuesMapNull = null;
		Map<String, TestValue> validTestValuesMapEmpty = new HashMap<>();
		Map<String, TestValue> validTestValuesMap = new HashMap<>();
		validTestValuesMap.put(testValue1.getName(), testValue1);
		validTestValuesMap.put(testValue1.getName(), testValue2);
		
		TypeValidationTestObject basicValidObjectWithAllPossibleNulls = new TypeValidationTestObject(validInt, validIntBoxedNull, validLong, validLongBoxedNull, 
				validFloat, validFloatBoxedNull, validDouble, validDoubleBoxedNull, validByte, validByteBoxedNull, validBoolean, validBooleanBoxedNull, 
				validStringNull, validIntArrayNull, validTestValuesListNull, validTestValuesArrayNull, validTestValuesMapNull, null);
		
		TypeValidationTestObject basicValidObjectWithEmptyCollections = new TypeValidationTestObject(validInt, validIntBoxed, validLong, validLongBoxed, 
				validFloat, validFloatBoxed, validDouble, validDoubleBoxed, validByte, validByteBoxed, validBoolean, validBooleanBoxed, 
				validString, validIntArrayEmpty, validTestValuesListEmpty, validTestValuesArrayEmpty, validTestValuesMapEmpty, basicValidObjectWithAllPossibleNulls);
		
		TypeValidationTestObject basicValidObjectWithBoxedValuesSwappedAndCollectionsSet = new TypeValidationTestObject(validIntBoxed, validInt, validLongBoxed, validLong, 
				validFloatBoxed, validFloat, validDoubleBoxed, validDouble, validByteBoxed, validByte, validBooleanBoxed, validBoolean, 
				validString, validIntArray, validTestValuesList, validTestValuesArray, validTestValuesMap, basicValidObjectWithEmptyCollections);
		
		//Add cycle to the object graph
		basicValidObjectWithAllPossibleNulls.setAnotherInstanceOfTheSameClass(basicValidObjectWithBoxedValuesSwappedAndCollectionsSet);
		
		try {
			ObjectValidation.validateFieldValueTypesForObject(basicValidObjectWithBoxedValuesSwappedAndCollectionsSet);
		} catch (SerializationException e) {
			e.printStackTrace();
			fail();
		}
		
		try {
			//Ensure that cycles are caught after serializing and deserializing an object that has a cycle
			ThreadLocalFstSerializer serializer = new ThreadLocalFstSerializer(new TestValidationConfigurationService());
			byte[] bytes = serializer.serializeObjectToByteArray(basicValidObjectWithBoxedValuesSwappedAndCollectionsSet);
			TypeValidationTestObject serializedClone = (TypeValidationTestObject) serializer.deserializeObjectFromByteArray(bytes);
			assertEquals(basicValidObjectWithBoxedValuesSwappedAndCollectionsSet, serializedClone);
			assertTrue(serializedClone == serializedClone.getAnotherInstanceOfTheSameClass().getAnotherInstanceOfTheSameClass().getAnotherInstanceOfTheSameClass());
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		TypeValidationTestObject selfReferencingObject = new TypeValidationTestObject(validInt, validIntBoxedNull, validLong, validLongBoxedNull, 
				validFloat, validFloatBoxedNull, validDouble, validDoubleBoxedNull, validByte, validByteBoxedNull, validBoolean, validBooleanBoxedNull, 
				validStringNull, validIntArrayNull, validTestValuesListNull, validTestValuesArrayNull, validTestValuesMapNull, null);
		//Add reference to self
		selfReferencingObject.setAnotherInstanceOfTheSameClass(selfReferencingObject);

		try {
			ObjectValidation.validateFieldValueTypesForObject(selfReferencingObject);
		} catch (SerializationException e) {
			e.printStackTrace();
			fail();
		}
		
		try {
			ObjectValidation.validateFieldValueTypesForObject(validTestValuesListWithNull);
		} catch (SerializationException e) {
			e.printStackTrace();
			fail();
		}
		
		try {
			ObjectValidation.validateFieldValueTypesForObject(validTestValuesArrayWithNull);
		} catch (SerializationException e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	public void testMassiveArrayOfValidCalls() throws IllegalArgumentException, IllegalAccessException, SerializationException {
		CallV2[] calls = new CallV2[100000];
		for(int i = 0; i < 100000; i++) {
			calls[i] = CallV2.generateBasicTestCall();
		}
		ObjectValidation.validateFieldValueTypesForObject(calls);
	}
	
	@Test
	public void testMassiveIterableOfValidCalls() throws IllegalArgumentException, IllegalAccessException, SerializationException {
		List<CallV2> calls = new LinkedList<>();
		for(int i = 0; i < 100000; i++) {
			calls.add(CallV2.generateBasicTestCall());
		}
		ObjectValidation.validateFieldValueTypesForObject(calls);
	}
	
	@Test
	public void testMassiveMapOfValidCalls() throws IllegalArgumentException, IllegalAccessException, SerializationException {
		Map<UUID, CallV2> calls = new HashMap<>();
		for(int i = 0; i < 100000; i++) {
			calls.put(UUID.randomUUID(), CallV2.generateBasicTestCall());
		}
		ObjectValidation.validateFieldValueTypesForObject(calls);
	}
	
	@Test
	public void testMassiveBlob() throws IllegalArgumentException, IllegalAccessException, SerializationException, InterruptedException {
		byte[] largeByteArray = new byte[100_000_000];
		new Random().nextBytes(largeByteArray);
		Blob blob = new Blob(UUID.randomUUID().toString(), "label", largeByteArray);
		
		TestTask testTask = new TestTask(() -> {
			ObjectValidation.validateFieldValueTypesForObject(blob);
		});
		testTask.run();
		testTask.awaitCompletion();
		testTask.printStackTraceIfErrorOccurred();
		
		assertTrue("An error ocurred while validating a large byte array. See logs for details.", testTask.getError() == null);
		assertTrue("It can't take forever to validate a large byte array", testTask.getDuration().getSeconds() < 5);
	}

	@Test
	public void testCorruptCall() throws SerializationException, IllegalArgumentException, IllegalAccessException, IOException, URISyntaxException {
		ThreadLocalFstSerializer serializer = new ThreadLocalFstSerializer(new TestValidationConfigurationService(), Call.getClassesToRegister());
		testCorruptObject(serializer, "corruptCall-1.bin");
	}

	@Test
	public void testCorruptCallInArray() throws SerializationException, IllegalArgumentException, IllegalAccessException, IOException, URISyntaxException {
		ThreadLocalFstSerializer serializer = new ThreadLocalFstSerializer(new TestValidationConfigurationService(), Call.getClassesToRegister());
		testCorruptObject(serializer, "corruptCallArrayList-1.bin");
	}

	private void testCorruptObject(BlueSerializer serializer, String relativeFilepathForCorruptObjectBytes) throws IOException, URISyntaxException {
		byte[] bytes = getResourceBytes(relativeFilepathForCorruptObjectBytes);
		try {
			serializer.deserializeObjectFromByteArray(bytes);
			Assert.fail();
		} catch(SerializationException e) {
			e.printStackTrace();
		}
	}
	
	private byte[] getResourceBytes(String filepath) throws IOException, URISyntaxException {
		return Files.readAllBytes(TestUtils.getResourcePath(filepath));
	}

	@SuppressWarnings("unused")
	@Test
	public void testValidateFieldValueType() throws Exception {
		Field booleanField = (new Object() {boolean  i;}).getClass().getDeclaredFields()[0];
		Field intField = (new Object() {int i;}).getClass().getDeclaredFields()[0];
		Field longField = (new Object() {long i;}).getClass().getDeclaredFields()[0];
		Field floatField = (new Object() {float i;}).getClass().getDeclaredFields()[0];
		Field doubleField = (new Object() {double i;}).getClass().getDeclaredFields()[0];
		Field byteField = (new Object() {byte i;}).getClass().getDeclaredFields()[0];
		Field charField = (new Object() {char i;}).getClass().getDeclaredFields()[0];
		Field shortField = (new Object() {short i;}).getClass().getDeclaredFields()[0];
		Field stringField = (new Object() {String i;}).getClass().getDeclaredFields()[0];

		ObjectValidation.validateFieldValueType(booleanField, true);
		ObjectValidation.validateFieldValueType(intField, (int) 1);
		ObjectValidation.validateFieldValueType(longField, (long) 1);
		ObjectValidation.validateFieldValueType(floatField, (float) 1);
		ObjectValidation.validateFieldValueType(doubleField, (double) 1);
		ObjectValidation.validateFieldValueType(byteField, (byte) 1);
		ObjectValidation.validateFieldValueType(charField, (char) 1);
		ObjectValidation.validateFieldValueType(shortField, (short) 1);
		ObjectValidation.validateFieldValueType(stringField, "");
		
		try {ObjectValidation.validateFieldValueType(booleanField, (int) 1); fail();} catch(SerializationException s) {}
		try {ObjectValidation.validateFieldValueType(intField, (long) 1); fail();} catch(SerializationException s) {}
		try {ObjectValidation.validateFieldValueType(longField, (float) 1); fail();} catch(SerializationException s) {}
		try {ObjectValidation.validateFieldValueType(floatField, (double) 1); fail();} catch(SerializationException s) {}
		try {ObjectValidation.validateFieldValueType(doubleField, (byte) 1); fail();} catch(SerializationException s) {}
		try {ObjectValidation.validateFieldValueType(byteField, (char) 1); fail();} catch(SerializationException s) {}
		try {ObjectValidation.validateFieldValueType(charField, (short) 1); fail();} catch(SerializationException s) {}
		try {ObjectValidation.validateFieldValueType(shortField, ""); fail();} catch(SerializationException s) {}
		try {ObjectValidation.validateFieldValueType(stringField, true); fail();} catch(SerializationException s) {}
	}

	@Test
	public void testConstructor() {
		new ObjectValidation(); // this test is stupid but needed to get 100% coverage
	}
}
