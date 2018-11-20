package io.bluedb.disk.serialization.validation;

import java.util.List;
import java.util.Map;

import io.bluedb.disk.TestValue;

public class TypeValidationTestObject {
	private int intField;
	private Integer intFieldBoxed;
	private long longField;
	private Long longFieldBoxed;
	private float floatField;
	private Float floatFieldBoxed;
	private double doubleField;
	private Double doubleFieldBoxed;
	private byte byteField;
	private Byte byteFieldBoxed;
	private boolean booleanField;
	private Boolean booleanFieldBoxed;
	private String stringField;
	private int[] intArray;
	private List<TestValue> testValuesList;
	private TestValue[] testValuesArray;
	private Map<String, TestValue> testValuesMap;
	private TypeValidationTestObject anotherInstanceOfTheSameClass;
	
	public TypeValidationTestObject(int intField, Integer intFieldBoxed, long longField, Long longFieldBoxed,
			float floatField, Float floatFieldBoxed, double doubleField, Double doubleFieldBoxed, byte byteField,
			Byte byteFieldBoxed, boolean booleanField, Boolean booleanFieldBoxed, String stringField, int[] intArray,
			List<TestValue> testValuesList, TestValue[] testValuesArray, Map<String, TestValue> testValuesMap,
			TypeValidationTestObject anotherInstanceOfTheSameClass) {
		this.intField = intField;
		this.intFieldBoxed = intFieldBoxed;
		this.longField = longField;
		this.longFieldBoxed = longFieldBoxed;
		this.floatField = floatField;
		this.floatFieldBoxed = floatFieldBoxed;
		this.doubleField = doubleField;
		this.doubleFieldBoxed = doubleFieldBoxed;
		this.byteField = byteField;
		this.byteFieldBoxed = byteFieldBoxed;
		this.booleanField = booleanField;
		this.booleanFieldBoxed = booleanFieldBoxed;
		this.stringField = stringField;
		this.intArray = intArray;
		this.testValuesList = testValuesList;
		this.testValuesArray = testValuesArray;
		this.testValuesMap = testValuesMap;
		this.anotherInstanceOfTheSameClass = anotherInstanceOfTheSameClass;
	}
	
	public void setAnotherInstanceOfTheSameClass(TypeValidationTestObject anotherInstanceOfTheSameClass) {
		this.anotherInstanceOfTheSameClass = anotherInstanceOfTheSameClass;
	}
}
