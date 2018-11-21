package io.bluedb.disk.serialization.validation;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

public class ObjectValidation {
	
	protected ObjectValidation() {} // just to get 100% test coverage

	public static void validateFieldValueTypesForObject(Object obj) throws IllegalArgumentException, IllegalAccessException, SerializationException {
		validateFieldValueTypesForObject(obj, new HashSet<>());
	}
	
	private static void validateFieldValueTypesForObject(Object obj, Set<Object> previouslyValidatedObjects) throws IllegalArgumentException, IllegalAccessException, SerializationException {
		if(obj == null) {
			return;
		}
		
		Field[] fields = obj.getClass().getDeclaredFields();
		if(!(isNullOrEmpty(fields))) {
			for(Field field : fields) {
				if(!Modifier.isStatic(field.getModifiers())) {
					field.setAccessible(true);
					Object fieldValue = field.get(obj);
					if(fieldValue != null) {
						validateFieldValueType(field, fieldValue);
						if(!field.getType().isPrimitive() && !previouslyValidatedObjects.contains(fieldValue)) {
							previouslyValidatedObjects.add(fieldValue);
							validateFieldValueTypesForObject(fieldValue, previouslyValidatedObjects);
						}
					}
				}
			}
		}
	}

	protected static boolean isNullOrEmpty(Object[] objects) {
		return objects == null || objects.length <= 0;
	}

	protected static void validateFieldValueType(Field field, Object value) throws SerializationException {
		Class<?> fieldType = field.getType();
		Class<? extends Object> valueType = value.getClass();
		
		if(fieldType.equals(boolean.class) && valueType.equals(Boolean.class)) {
			return;
		}
		if(fieldType.equals(int.class) && valueType.equals(Integer.class)) {
			return;
		}
		if(fieldType.equals(long.class) && valueType.equals(Long.class)) {
			return;
		}
		if(fieldType.equals(float.class) && valueType.equals(Float.class)) {
			return;
		}
		if(fieldType.equals(double.class) && valueType.equals(Double.class)) {
			return;
		}
		if(fieldType.equals(byte.class) && valueType.equals(Byte.class)) {
			return;
		}
		if(fieldType.equals(char.class) && valueType.equals(Character.class)) {
			return;
		}
		if(fieldType.equals(short.class) && valueType.equals(Short.class)) {
			return;
		}
		if(!fieldType.isAssignableFrom(valueType)) {
			throw new SerializationException("Field " + field + " cannot hold a value of type " + valueType);
		}
	}
}
