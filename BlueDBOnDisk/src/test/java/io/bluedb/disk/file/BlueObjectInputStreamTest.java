package io.bluedb.disk.file;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.serialization.BlueSerializer;
import io.bluedb.disk.serialization.ThreadLocalFstSerializer;
import junit.framework.TestCase;

public class BlueObjectInputStreamTest extends TestCase {

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
		// TODO
	}

	@Test
	public void test_next() {
		TestValue value = new TestValue("Jobodo Monobodo");
		BlueObjectInputStream<TestValue> inStream = null;
		try {
			BlueObjectOutputStream<TestValue> outStream = fileManager.getBlueOutputStream(targetFilePath);
			outStream.write(value);
			outStream.commit();

			inStream = fileManager.getBlueInputStream(targetFilePath);
			assertEquals(value, inStream.next());
		} catch (BlueDbException | IOException e) {
			e.printStackTrace();
			fail();
		}
		try {
			inStream.next();
			fail(); // already took in the only value in there.
		} catch (EOFException eof) {
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}
}
