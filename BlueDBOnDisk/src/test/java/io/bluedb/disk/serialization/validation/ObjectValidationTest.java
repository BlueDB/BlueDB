package io.bluedb.disk.serialization.validation;

import static org.junit.Assert.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import io.bluedb.disk.TestValue;
import io.bluedb.disk.models.calls.Call;
import io.bluedb.disk.serialization.ThreadLocalFstSerializer;

public class ObjectValidationTest {

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
		
		TestValue[] validTestValuesArrayNull = null;
		TestValue[] validTestValuesArrayEmpty = new TestValue[] { };
		TestValue[] validTestValuesArray = new TestValue[] { testValue1, testValue2 };
		
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
		} catch (IllegalArgumentException | IllegalAccessException | SerializationException e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	public void testInvalidObjects() throws SerializationException, IllegalArgumentException, IllegalAccessException, IOException, URISyntaxException {
		ThreadLocalFstSerializer serializer = new ThreadLocalFstSerializer(Call.getClassesToRegister());
		
		testCorruptCall("corruptCall-1.bin", serializer);
	}

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

	private void testCorruptCall(String filename, ThreadLocalFstSerializer serializer) throws IOException, URISyntaxException, SerializationException, IllegalArgumentException, IllegalAccessException {
		byte[] bytes = Files.readAllBytes(Paths.get(this.getClass().getResource("/" + filename).toURI()));
		try {
			serializer.deserializeObjectFromByteArray(bytes);
			Assert.fail();
		} catch(SerializationException e) {
			e.printStackTrace();
		}
	}
}
