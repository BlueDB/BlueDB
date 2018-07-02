package io.bluedb.disk.file;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.serialization.BlueSerializer;
import io.bluedb.disk.serialization.ThreadLocalFstSerializer;
import junit.framework.TestCase;

public class BlueObjectOutputStreamTest extends TestCase {

	BlueSerializer serializer;
	FileManager fileManager;
	Path testingFolder = Paths.get(".testing");
	Path targetFilePath = Paths.get(testingFolder.toString(), "BlueObjectOutputStreamTest.test_junk");
	Path tempFilePath = FileManager.createTempFilePath(targetFilePath);

	@Override
	protected void setUp() throws Exception {
		serializer = new ThreadLocalFstSerializer(new Class[]{});
		fileManager = new FileManager(serializer);
	}

	@Override
	protected void tearDown() throws Exception {
		targetFilePath.toFile().delete();
		tempFilePath.toFile().delete();
		testingFolder.toFile().delete();
	}

	@Test
	public void test_close() {
		try {
			BlueObjectOutputStream<TestValue> stream = fileManager.getBlueOutputStream(targetFilePath);
			stream.write(null);
			stream.close();
			stream.close();  // make sure it doesn't throw an exception if you close it twice
			assertTrue(tempFilePath.toFile().exists()); // we haven't moved it to the target file name yet
			assertFalse(targetFilePath.toFile().exists()); // we haven't moved it to the target file name yet
		} catch (BlueDbException | IOException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_write() {
		TestValue value = new TestValue("Jobodo Monobodo");
		try {
			BlueObjectOutputStream<TestValue> outStream = fileManager.getBlueOutputStream(targetFilePath);
			outStream.write(value);
			outStream.commit();

			BlueObjectInputStream<TestValue> inStream = fileManager.getBlueInputStream(targetFilePath);
			assertEquals(value, inStream.next());
		} catch (BlueDbException | IOException e) {
			e.printStackTrace();
			fail();
		}
		// TODO test exception on write
	}

	@Test
	public void test_commit() {
		TestValue value = new TestValue("Jobodo Monobodo");
		try {
			BlueObjectOutputStream<TestValue> outStream = fileManager.getBlueOutputStream(targetFilePath);
			outStream.write(value);
			outStream.commit();

			BlueObjectInputStream<TestValue> inStream = fileManager.getBlueInputStream(targetFilePath);
			assertEquals(value, inStream.next());
		} catch (BlueDbException | IOException e) {
			e.printStackTrace();
			fail();
		}
	}
}
