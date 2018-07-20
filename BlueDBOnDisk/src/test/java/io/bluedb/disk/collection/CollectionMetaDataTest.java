package io.bluedb.disk.collection;

import static org.junit.Assert.assertArrayEquals;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
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
		metaData = getCollection().getMetaData();
	}

	@Test
	public void test_getMaxLong() {
		try {
			assertNull(metaData.getMaxLong());
			metaData.updateMaxLong(1);
			assertEquals(1, 1L);
			metaData.updateMaxLong(3);
			assertEquals(3, metaData.getMaxLong().longValue());
			metaData.updateMaxLong(2);
			assertEquals(3, metaData.getMaxLong().longValue());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_getMaxInteger() {
		try {
			assertNull(metaData.getMaxInteger());
			metaData.updateMaxInteger(1);
			assertEquals(1, 1L);
			metaData.updateMaxInteger(3);
			assertEquals(3, metaData.getMaxInteger().longValue());
			metaData.updateMaxInteger(2);
			assertEquals(3, metaData.getMaxInteger().longValue());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_updateMaxLong() {
		try {
			assertNull(metaData.getMaxLong());
			metaData.updateMaxLong(1);
			assertEquals(1, 1L);
			metaData.updateMaxLong(3);
			assertEquals(3, metaData.getMaxLong().longValue());
			metaData.updateMaxLong(2);
			assertEquals(3, metaData.getMaxLong().longValue());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_updateMaxInteger() {
		try {
			assertNull(metaData.getMaxInteger());
			metaData.updateMaxInteger(1);
			assertEquals(1, 1L);
			metaData.updateMaxInteger(3);
			assertEquals(3, metaData.getMaxInteger().longValue());
			metaData.updateMaxInteger(2);
			assertEquals(3, metaData.getMaxInteger().longValue());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_getSerializedClassList() {
		metaData = createNewMetaData();  // use fresh metadata so collection startup doesn't change things
		try {
			assertNull(metaData.getSerializedClassList());
			List<Class<? extends Serializable>> classes = Arrays.asList(TestValue.class);
			metaData.updateSerializedClassList(classes);
			assertEquals(classes, metaData.getSerializedClassList());
			classes = Arrays.asList(TestValue.class, TestValue2.class);
			assertFalse(classes.equals(metaData.getSerializedClassList())); // we haven't synced them yet
			metaData.updateSerializedClassList(classes);
			assertTrue(classes.equals(metaData.getSerializedClassList()));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_getSerializedClassList_exception() {
		try {
			Path serializedClassesPath = Paths.get(metaData.getPath().toString(), "serialized_classes");
			getFileManager().saveObject(serializedClassesPath, "some_nonsense");  // serialize a string where there should be a list
			metaData.getSerializedClassList();  // now this should fail with BlueDbException
			fail();
		} catch (BlueDbException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test_updateSerializedClassList() {
		metaData = createNewMetaData();  // use fresh metadata so collection startup doesn't change things
		try {
			assertNull(metaData.getSerializedClassList());
			List<Class<? extends Serializable>> classes = Arrays.asList(TestValue.class);
			metaData.updateSerializedClassList(classes);
			assertEquals(classes, metaData.getSerializedClassList());
			classes = Arrays.asList(TestValue.class, TestValue2.class);
			assertFalse(classes.equals(metaData.getSerializedClassList())); // we haven't synced them yet
			metaData.updateSerializedClassList(classes);
			assertTrue(classes.equals(metaData.getSerializedClassList()));
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_getAndAddToSerializedClassList() {
		metaData = createNewMetaData();  // use fresh metadata so collection startup doesn't change things
		try {
			assertNull(metaData.getSerializedClassList());
			Class<? extends Serializable>[] testValue1 = new Class[] {TestValue.class};
			Class<? extends Serializable>[] testValueBoth = new Class[] {TestValue.class, TestValue2.class};

			Class<? extends Serializable>[] afterAdding1 = metaData.getAndAddToSerializedClassList(testValue1);
			assertArrayEquals(testValue1, afterAdding1);

			Class<? extends Serializable>[] afterAdding1Again = metaData.getAndAddToSerializedClassList(testValue1);
			assertArrayEquals(testValue1, afterAdding1Again);

			Class<? extends Serializable>[] afterAddingBoth = metaData.getAndAddToSerializedClassList(testValueBoth);
			assertArrayEquals(testValueBoth, afterAddingBoth);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	private CollectionMetaData createNewMetaData() {
		Path tempPath = createTempPath();
		return new CollectionMetaData(tempPath);
	}

	private Path createTempPath() {
		try {
			Path tempPath = Files.createTempDirectory(this.getClass().getSimpleName());
			tempPath.toFile().deleteOnExit();
			return tempPath;
		} catch (IOException e) {
			e.printStackTrace();
			fail();
			return null; // fail() will prevent this from being called but need it to compile
		}
	}
}
