package io.bluedb.disk.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.serialization.BlueSerializer;
import io.bluedb.disk.serialization.ThreadLocalFstSerializer;
import junit.framework.TestCase;

public class FileManagerTest  extends TestCase {

	BlueSerializer serializer;
	FileManager fileManager;

	@Override
	protected void setUp() throws Exception {
		serializer = new ThreadLocalFstSerializer(new Class[]{});
		fileManager = new FileManager(serializer);
	}

	// TODO test multiple files and/or files with suffix not at the end
	@Test
	public void test_listFiles() {
		String suffix = ".foo";
		File nonExistant = new File("forever_or_never_whatever");
		List<File> emptyFileList = new ArrayList<>();
		assertEquals(emptyFileList, fileManager.listFiles(nonExistant.toPath(), suffix));

		File emptyFolder = new File("bah_bah_black_sheep");
		emptyFolder.mkdirs();
		assertEquals(emptyFileList, fileManager.listFiles(emptyFolder.toPath(), suffix));

		File nonEmptyFolder = new File("owa_tana_siam");
		nonEmptyFolder.mkdirs();
		File fileWithSuffix = createFile(nonEmptyFolder, "legit" + suffix);
		createFile(nonEmptyFolder, "junk");
		List<File> filesWithSuffix = fileManager.listFiles(nonEmptyFolder.toPath(), suffix);
		assertEquals(1, filesWithSuffix.size());
		assertTrue(filesWithSuffix.contains(fileWithSuffix));
		
		recursiveDelete(emptyFolder);
		recursiveDelete(nonEmptyFolder);
	}

	@Test
	public void test_loadObject() {
		TestValue value = new TestValue("joe", 1);
		File nonExistantFile = new File("nonExistanceTestFile");
		File emptyFile = createFile("emptyTestFile");
		File corruptedFile = createCorruptedFile("corruptedTestFile");
		File fileWithValue = new File("testFileWithValue");
		try {
			fileManager.saveObject(fileWithValue.toPath(), value);

			Object nonExistantObject = fileManager.loadObject(nonExistantFile.toPath());
			Object emptyObject = fileManager.loadObject(emptyFile.toPath());
			Object validObject = fileManager.loadObject(fileWithValue.toPath());
			assertNull(nonExistantObject);
			assertNull(emptyObject);
			assertEquals(value, validObject);
			
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		
		recursiveDelete(emptyFile);
		recursiveDelete(corruptedFile);
		recursiveDelete(fileWithValue);
	}

	@Test
	public void test_saveObject() {
		TestValue value = new TestValue("joe", 1);
		File fileWithNull = createFile("testFileWithNull");
		File fileWithValue = new File("testFileWithValue");

		try {
			fileManager.saveObject(fileWithValue.toPath(), value);
			Object reloadedObject = fileManager.loadObject(fileWithValue.toPath());
			assertEquals(value, reloadedObject);
			
			fileManager.saveObject(fileWithNull.toPath(), null);
			Object reloadedNull = fileManager.loadObject(fileWithNull.toPath());
			assertNull(reloadedNull);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}

		recursiveDelete(fileWithNull);
		recursiveDelete(fileWithValue);
	}

	@Test
	public void test_multithreaded() {
		TestValue value = new TestValue("joe", 0);
		File file = createFileAndWriteTestValue("testMultithreaded", value);
		Path path = file.toPath();

		int attemptsToOpen = 500;
		CountDownLatch doneSignal = new CountDownLatch(attemptsToOpen);
		AtomicLong successfulOpens = new AtomicLong(0);

		List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < attemptsToOpen; i++) {
			Runnable reader = new Runnable() {
				@Override public void run() {
					TestValue loadedValue = loadTestValueFromPathOrNull(path);
					if (loadedValue != null) {
						successfulOpens.incrementAndGet();
					}
					doneSignal.countDown();
				}
			};
			Thread readerThread = new Thread(reader);
			threads.add(readerThread);
		}
		
		// interleave writing and reading
		while(doneSignal.getCount() > 0) {
			if (!threads.isEmpty()) {
				Thread thread = threads.remove(threads.size()-1);
				thread.start();
			}
			loadAddCupcakeAndSave(path);
		}

		assertEquals(attemptsToOpen, successfulOpens.get());

		recursiveDelete(file);
	}

	private void recursiveDelete(File file) {
		if (!file.exists()) {
			return;
		} else if (file.isDirectory()) {
			for (File f: file.listFiles()) {
				recursiveDelete(f);
			}
			file.delete();
		} else {
			file.delete();
		}
	}

	private File createFile(String fileName) {
		File file = new File(fileName);
		try {
			file.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		return file;
	}

	private File createCorruptedFile(String fileName) {
		File file = new File(fileName);
		byte[] junk = new byte[] {3, 1, 2};
		try (FileOutputStream fos = new FileOutputStream(file)) {
			fos.write(junk);
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		return file;
	}

	private File createFile(File parentFolder, String fileName) {
		File file = Paths.get(parentFolder.toPath().toString(), fileName).toFile();
		try {
			file.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		return file;
	}

	private File createFileAndWriteTestValue(String pathString, TestValue value) {
		File file = new File(pathString);
		try {
			fileManager.saveObject(file.toPath(), value);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		return file;
	}

	private void loadAddCupcakeAndSave(Path path) {
		try {
			TestValue value = (TestValue) fileManager.loadObject(path);
			value.addCupcake();
			fileManager.saveObject(path, value);
		} catch (BlueDbException e) {
			System.out.println("attempt to save failed");
		}
	}

	private TestValue loadTestValueFromPathOrNull(Path path) {
		try {
			TestValue value = (TestValue) fileManager.loadObject(path);
			return value;
		} catch (BlueDbException e) {
			e.printStackTrace();
		}
		return null;
	}
}

