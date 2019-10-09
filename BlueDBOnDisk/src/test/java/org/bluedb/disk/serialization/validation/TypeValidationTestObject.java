package org.bluedb.disk.serialization.validation;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.bluedb.disk.TestValue;

public class TypeValidationTestObject implements Serializable {
	private static final long serialVersionUID = 1L;
	
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
	
	public TypeValidationTestObject getAnotherInstanceOfTheSameClass() {
		return anotherInstanceOfTheSameClass;
	}
	
	public void setAnotherInstanceOfTheSameClass(TypeValidationTestObject anotherInstanceOfTheSameClass) {
		this.anotherInstanceOfTheSameClass = anotherInstanceOfTheSameClass;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (booleanField ? 1231 : 1237);
		result = prime * result + ((booleanFieldBoxed == null) ? 0 : booleanFieldBoxed.hashCode());
		result = prime * result + byteField;
		result = prime * result + ((byteFieldBoxed == null) ? 0 : byteFieldBoxed.hashCode());
		long temp;
		temp = Double.doubleToLongBits(doubleField);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((doubleFieldBoxed == null) ? 0 : doubleFieldBoxed.hashCode());
		result = prime * result + Float.floatToIntBits(floatField);
		result = prime * result + ((floatFieldBoxed == null) ? 0 : floatFieldBoxed.hashCode());
		result = prime * result + Arrays.hashCode(intArray);
		result = prime * result + intField;
		result = prime * result + ((intFieldBoxed == null) ? 0 : intFieldBoxed.hashCode());
		result = prime * result + (int) (longField ^ (longField >>> 32));
		result = prime * result + ((longFieldBoxed == null) ? 0 : longFieldBoxed.hashCode());
		result = prime * result + ((stringField == null) ? 0 : stringField.hashCode());
		result = prime * result + Arrays.hashCode(testValuesArray);
		result = prime * result + ((testValuesList == null) ? 0 : testValuesList.hashCode());
		result = prime * result + ((testValuesMap == null) ? 0 : testValuesMap.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TypeValidationTestObject other = (TypeValidationTestObject) obj;
		if (booleanField != other.booleanField)
			return false;
		if (booleanFieldBoxed == null) {
			if (other.booleanFieldBoxed != null)
				return false;
		} else if (!booleanFieldBoxed.equals(other.booleanFieldBoxed))
			return false;
		if (byteField != other.byteField)
			return false;
		if (byteFieldBoxed == null) {
			if (other.byteFieldBoxed != null)
				return false;
		} else if (!byteFieldBoxed.equals(other.byteFieldBoxed))
			return false;
		if (Double.doubleToLongBits(doubleField) != Double.doubleToLongBits(other.doubleField))
			return false;
		if (doubleFieldBoxed == null) {
			if (other.doubleFieldBoxed != null)
				return false;
		} else if (!doubleFieldBoxed.equals(other.doubleFieldBoxed))
			return false;
		if (Float.floatToIntBits(floatField) != Float.floatToIntBits(other.floatField))
			return false;
		if (floatFieldBoxed == null) {
			if (other.floatFieldBoxed != null)
				return false;
		} else if (!floatFieldBoxed.equals(other.floatFieldBoxed))
			return false;
		if (!Arrays.equals(intArray, other.intArray))
			return false;
		if (intField != other.intField)
			return false;
		if (intFieldBoxed == null) {
			if (other.intFieldBoxed != null)
				return false;
		} else if (!intFieldBoxed.equals(other.intFieldBoxed))
			return false;
		if (longField != other.longField)
			return false;
		if (longFieldBoxed == null) {
			if (other.longFieldBoxed != null)
				return false;
		} else if (!longFieldBoxed.equals(other.longFieldBoxed))
			return false;
		if (stringField == null) {
			if (other.stringField != null)
				return false;
		} else if (!stringField.equals(other.stringField))
			return false;
		if (!Arrays.equals(testValuesArray, other.testValuesArray))
			return false;
		if (testValuesList == null) {
			if (other.testValuesList != null)
				return false;
		} else if (!testValuesList.equals(other.testValuesList))
			return false;
		if (testValuesMap == null) {
			if (other.testValuesMap != null)
				return false;
		} else if (!testValuesMap.equals(other.testValuesMap))
			return false;
		return true;
	}
}
