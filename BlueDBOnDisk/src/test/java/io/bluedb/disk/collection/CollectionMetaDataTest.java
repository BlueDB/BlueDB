package io.bluedb.disk.collection;

import static org.junit.Assert.assertArrayEquals;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.BlueDbDiskTestBase;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.TestValue2;

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

		assertNull(metaData.getSerializedClassList());
		Class<? extends Serializable>[] testValue1 = new Class[] {TestValue.class};
		Class<? extends Serializable>[] testValueBoth = new Class[] {TestValue.class, TestValue2.class};

		Class<? extends Serializable>[] afterAdding1 = metaData.getAndAddToSerializedClassList(TestValue.class);
		assertArrayEquals(testValue1, afterAdding1);

		Class<? extends Serializable>[] afterAdding1Again = metaData.getAndAddToSerializedClassList(TestValue.class);
		assertArrayEquals(testValue1, afterAdding1Again);

		Class<? extends Serializable>[] afterAddingBoth = metaData.getAndAddToSerializedClassList(TestValue.class, TestValue2.class);
		assertArrayEquals(testValueBoth, afterAddingBoth);
	}

	private CollectionMetaData createNewMetaData() {
		Path tempPath = createTempFolder().toPath();
		return new CollectionMetaData(tempPath);
	}
}
