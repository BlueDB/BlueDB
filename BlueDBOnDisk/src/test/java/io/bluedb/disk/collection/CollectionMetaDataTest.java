package io.bluedb.disk.collection;

import static org.junit.Assert.assertArrayEquals;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.junit.Test;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.TestValue2;
import io.bluedb.disk.serialization.ThreadLocalFstSerializerPair;

public class CollectionMetaDataTest extends BlueDbDiskTestBase {

	private CollectionMetaData metaData;
	
	@Override
	public void setUp() throws Exception {
		super.setUp();
		metaData = getTimeCollection().getMetaData();
	}

	@Test
	public void test_getSerializedClassList() throws Exception {
		metaData = createNewMetaData();  // use fresh metadata so collection startup doesn't change things
		assertNull(metaData.getSerializedClassList());
		List<Class<? extends Serializable>> classes = Arrays.asList(TestValue.class);
		metaData.updateSerializedClassList(classes);
		assertEquals(classes, metaData.getSerializedClassList());
		classes = Arrays.asList(TestValue.class, TestValue2.class);
		assertFalse(classes.equals(metaData.getSerializedClassList())); // we haven't synced them yet
		metaData.updateSerializedClassList(classes);
		assertTrue(classes.equals(metaData.getSerializedClassList()));
	}

	@Test
	public void test_getSerializedClassList_exception() throws Exception {
		Path serializedClassesPath = Paths.get(metaData.getPath().toString(), "serialized_classes");
		getFileManager().saveObject(serializedClassesPath, "some_nonsense");  // serialize a string where there should be a list
		try {
			metaData.getSerializedClassList();  // now this should fail with BlueDbException
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_updateSerializedClassList() throws Exception {
		metaData = createNewMetaData();  // use fresh metadata so collection startup doesn't change things
		assertNull(metaData.getSerializedClassList());
		List<Class<? extends Serializable>> classes = Arrays.asList(TestValue.class);
		metaData.updateSerializedClassList(classes);
		assertEquals(classes, metaData.getSerializedClassList());
		classes = Arrays.asList(TestValue.class, TestValue2.class);
		assertFalse(classes.equals(metaData.getSerializedClassList())); // we haven't synced them yet
		metaData.updateSerializedClassList(classes);
		assertTrue(classes.equals(metaData.getSerializedClassList()));
	}

	@Test
	public void test_getAndAddToSerializedClassList() throws Exception {
		metaData = createNewMetaData();  // use fresh metadata so collection startup doesn't change things
		Class<? extends Serializable>[] defaultClasses = getClassesToAlwaysRegister();

		assertNull(metaData.getSerializedClassList());
		@SuppressWarnings("unchecked")
		Class<? extends Serializable>[] testValue1 = new Class[] {TestValue.class};
		@SuppressWarnings("unchecked")
		Class<? extends Serializable>[] testValueBoth = new Class[] {TestValue.class, TestValue2.class};
		Class<? extends Serializable>[] testValue1PlusDefaults = concatenate(defaultClasses, testValue1);
		Class<? extends Serializable>[] testValueBothPlusDefaults =  concatenate(defaultClasses, testValueBoth);

		Class<? extends Serializable>[] afterAdding1 = metaData.getAndAddToSerializedClassList(TestValue.class);
		assertArrayEquals(testValue1PlusDefaults, afterAdding1);

		Class<? extends Serializable>[] afterAdding1Again = metaData.getAndAddToSerializedClassList(TestValue.class);
		assertArrayEquals(testValue1PlusDefaults, afterAdding1Again);

		Class<? extends Serializable>[] afterAddingBoth = metaData.getAndAddToSerializedClassList(TestValue.class, TestValue2.class);
		assertArrayEquals(testValueBothPlusDefaults, afterAddingBoth);
	}

	private Class<? extends Serializable>[] getClassesToAlwaysRegister() {
		Collection<? extends Class<? extends Serializable>> classesToAlwaysRegister = ThreadLocalFstSerializerPair.getClassesToAlwaysRegister();
		@SuppressWarnings("unchecked")
		Class<? extends Serializable>[] array = new Class[classesToAlwaysRegister.size()];
		classesToAlwaysRegister.toArray(array);
		return array;
	}

	private <T> T[] concatenate(T[] a, T[] b) {
		int combinedLength = a.length + b.length;
		T[] combined = Arrays.copyOf(a, combinedLength);
	    System.arraycopy(b, 0, combined, a.length, b.length);
	    return combined;
	}

	private CollectionMetaData createNewMetaData() {
		Path tempPath = createTempFolder().toPath();
		return new CollectionMetaData(tempPath);
	}
}
