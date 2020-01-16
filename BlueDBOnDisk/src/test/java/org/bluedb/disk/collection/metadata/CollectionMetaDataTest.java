package org.bluedb.disk.collection.metadata;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.TestValue2;
import org.bluedb.disk.TestValueSub;
import org.bluedb.disk.collection.metadata.CollectionMetaData;
import org.bluedb.disk.file.FileManager;
import org.bluedb.disk.segment.SegmentSizeSetting;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;
import org.junit.Test;

public class CollectionMetaDataTest extends BlueDbDiskTestBase {

	private CollectionMetaData metaData;
	
	@Override
	public void setUp() throws Exception {
		super.setUp();
		metaData = getTimeCollection().getMetaData();
	}

	@Test
	public void test_segmentSize() throws Exception {
		SegmentSizeSetting size1 = SegmentSizeSetting.HASH_128K;
		SegmentSizeSetting size2 = SegmentSizeSetting.TIME_2_HOURS;
		metaData = createNewMetaData();  // use fresh metadata so collection startup doesn't change things
		assertNull(metaData.getSegmentSize());
		metaData.saveSegmentSize(size1);
		assertEquals(size1, metaData.getSegmentSize());
		metaData.saveSegmentSize(size2);
		assertEquals(size2, metaData.getSegmentSize());
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
		Path serializedClassesPath = FileManager.getNewestVersionPath(metaData.getPath(), CollectionMetaData.FILENAME_SERIALIZED_CLASSES);
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
		
		@SuppressWarnings("unchecked")
		Class<? extends Serializable>[] testValue1 = new Class[] {TestValue.class};
		
		@SuppressWarnings("unchecked")
		Class<? extends Serializable>[] testValues = new Class[] {TestValue.class, TestValue2.class, TestValueSub.class};
		
		Class<? extends Serializable>[] testValue1PlusDefaults = concatenate(defaultClasses, testValue1);
		
		Class<? extends Serializable>[] testValuesPlusDefaults =  concatenate(defaultClasses, testValues);
		
		List<Class<? extends Serializable>> testValue1PlusDefaultsShuffled = shuffle(testValue1PlusDefaults);
		List<Class<? extends Serializable>> testValuesPlusDefaultsShuffled = shuffle(testValuesPlusDefaults);
		
		assertNull(metaData.getSerializedClassList()); //Doesn't exist to start

		Class<? extends Serializable>[] afterAdding1 = metaData.getAndAddToSerializedClassList(TestValue.class, Arrays.asList());
		Path file1Path = FileManager.getNewestVersionPath(metaData.folderPath, CollectionMetaData.FILENAME_SERIALIZED_CLASSES);
		FileTime lastModifiedTime1 = Files.getLastModifiedTime(file1Path);
		assertArrayEquals(testValue1PlusDefaults, afterAdding1); //Now contains defaults and value 1

		Class<? extends Serializable>[] afterAdding1Again = metaData.getAndAddToSerializedClassList(TestValue.class, Arrays.asList());
		assertArrayEquals(testValue1PlusDefaults, afterAdding1Again); //Shouldn't change
		assertEquals(lastModifiedTime1, Files.getLastModifiedTime(file1Path)); //Shouldn't be overwritten
		assertEquals(file1Path, FileManager.getNewestVersionPath(metaData.folderPath, CollectionMetaData.FILENAME_SERIALIZED_CLASSES)); //Newest file should still be the same

		Class<? extends Serializable>[] afterAdding1AgainInWeirdOrder = metaData.getAndAddToSerializedClassList(TestValue.class, testValue1PlusDefaultsShuffled);
		assertArrayEquals(testValue1PlusDefaults, afterAdding1AgainInWeirdOrder); //Shouldn't change
		assertEquals(lastModifiedTime1, Files.getLastModifiedTime(file1Path)); //Shouldn't be overwritten
		assertEquals(file1Path, FileManager.getNewestVersionPath(metaData.folderPath, CollectionMetaData.FILENAME_SERIALIZED_CLASSES)); //Newest file should still be the same

		Class<? extends Serializable>[] afterAddingValues = metaData.getAndAddToSerializedClassList(TestValue.class, Arrays.asList(TestValue2.class, TestValueSub.class));
		Path file2Path = FileManager.getNewestVersionPath(metaData.folderPath, CollectionMetaData.FILENAME_SERIALIZED_CLASSES);
		FileTime lastModifiedTime2 = Files.getLastModifiedTime(file2Path);
		assertArrayEquals(testValuesPlusDefaults, afterAddingValues); //Contains all values now
		assertEquals(lastModifiedTime1, Files.getLastModifiedTime(file1Path)); //Original file shouldn't have changed
		assertNotEquals(file1Path, file2Path); //Newest file is no longer the first file
		
		Class<? extends Serializable>[] afterAddingValuesBackwards = metaData.getAndAddToSerializedClassList(TestValue.class, Arrays.asList(TestValueSub.class, TestValue2.class));
		assertArrayEquals(testValuesPlusDefaults, afterAddingValuesBackwards); //Shouldn't change
		assertEquals(lastModifiedTime1, Files.getLastModifiedTime(file1Path)); //Shouldn't change
		assertEquals(lastModifiedTime2, Files.getLastModifiedTime(file2Path)); //Shouldn't change
		assertEquals(file2Path, FileManager.getNewestVersionPath(metaData.folderPath, CollectionMetaData.FILENAME_SERIALIZED_CLASSES)); //Newest file should still be the same
		
		Class<? extends Serializable>[] afterAddingEverythingInWeirdOrder = metaData.getAndAddToSerializedClassList(TestValue.class, testValuesPlusDefaultsShuffled);
		assertArrayEquals(testValuesPlusDefaults, afterAddingEverythingInWeirdOrder); //Shouldn't change
		assertEquals(lastModifiedTime1, Files.getLastModifiedTime(file1Path)); //Shouldn't change
		assertEquals(lastModifiedTime2, Files.getLastModifiedTime(file2Path)); //Shouldn't change
		assertEquals(file2Path, FileManager.getNewestVersionPath(metaData.folderPath, CollectionMetaData.FILENAME_SERIALIZED_CLASSES)); //Newest file should still be the same
	}

	private Class<? extends Serializable>[] getClassesToAlwaysRegister() {
		Collection<? extends Class<? extends Serializable>> classesToAlwaysRegister = ThreadLocalFstSerializer.getClassesToAlwaysRegister();
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

	private <T> List<T> shuffle(T[] array) {
		List<T> list = new ArrayList<>(Arrays.asList(array));
		Collections.shuffle(list);
		return list;
	}

	private CollectionMetaData createNewMetaData() {
		Path tempPath = createTempFolder().toPath();
		return new CollectionMetaData(tempPath);
	}
}
