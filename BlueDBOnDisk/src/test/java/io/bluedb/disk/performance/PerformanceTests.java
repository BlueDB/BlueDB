package io.bluedb.disk.performance;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.bluedb.disk.TestValue;
import io.bluedb.disk.serialization.BlueSerializer;
import io.bluedb.disk.serialization.ThreadLocalFstSerializerPair;

public class PerformanceTests {

	private static final int NUMBER_OF_VALUES = 5_000;
	
	private Path tempDir;
	private Path file;
	private BlueSerializer serializer;
	private List<TestValue> values;
	private TestResults results = new TestResults();

	public void setUp(boolean registerClass) throws Exception {
		tempDir = Files.createTempDirectory("BlueDbOnDiskTest");
		file = tempDir.resolve("test.bin");

		if(registerClass) {
			serializer = new ThreadLocalFstSerializerPair(TestValue.class);
		}
		else {
			serializer = new ThreadLocalFstSerializerPair();
		}
		
		Random r = new Random();
		values = IntStream.range(0, NUMBER_OF_VALUES)
				.boxed()
				.map(i -> new TestValue(UUID.randomUUID().toString(), r.nextInt()))
				.collect(Collectors.toCollection(LinkedList::new));
		
		results = new TestResults();
	}

	public void tearDown() throws Exception {
		Files.walk(tempDir)
		.sorted(Comparator.reverseOrder())
		.map(Path::toFile)
		.forEach(File::delete);
	}
	
	public void testOneObjectAtATime() throws IOException, ClassNotFoundException {
		results.writeStart = Instant.now();
		serializeListToFileOneObjectAtATime();
		results.writeEnd = Instant.now();

		results.sizeOnDisk = Files.size(file);

		results.readStart = Instant.now();
		List<TestValue> deserializedValues = deserializeListFromFileOneObjectAtATime();
		results.readEnd = Instant.now();

		assertEquals(values, deserializedValues);
		results.print("testOneObjectAtATime");
	}

//	public void testOneObjectAtATimeUsingObjectStream() throws IOException, ClassNotFoundException {
//		results.writeStart = Instant.now();
//		serializeListToFileOneObjectAtATimeInObjectStream();
//		results.writeEnd = Instant.now();
//	
//		results.sizeOnDisk = Files.size(file);
//	
//		results.readStart = Instant.now();
//		List<TestValue> deserializedValues = deserializeListFromFileOneObjectAtATimeInObjectStream();
//		results.readEnd = Instant.now();
//	
//		assertEquals(values, deserializedValues);
//		results.print("testOneObjectAtATimeUsingObjectStream");
//	}

	public void testAsList() throws IOException, ClassNotFoundException {
		results.writeStart = Instant.now();
		serializeObject(values);
		results.writeEnd = Instant.now();

		results.sizeOnDisk = Files.size(file);

		results.readStart = Instant.now();
		@SuppressWarnings("unchecked")
		List<TestValue> deserializedValues = (List<TestValue>) deserializeObject();
		results.readEnd = Instant.now();

		assertEquals(values, deserializedValues);
		results.print("testAsList");
	}

	public void testAsArray() throws IOException, ClassNotFoundException {
		TestValue[] valuesArray = new TestValue[values.size()];
		values.toArray(valuesArray);
		
		results.writeStart = Instant.now();
		serializeObject(valuesArray);
		results.writeEnd = Instant.now();

		results.sizeOnDisk = Files.size(file);

		results.readStart = Instant.now();
		TestValue[] deserializedValues = (TestValue[]) deserializeObject();
		results.readEnd = Instant.now();

		assertEquals(values, Arrays.asList(deserializedValues));
		results.print("testAsArray");
	}

	@SuppressWarnings("unchecked")
//	public void testAsListUsingObjectStream() throws IOException, ClassNotFoundException {
//		results.writeStart = Instant.now();
//		serializeObjectUsingObjectStream(values);
//		results.writeEnd = Instant.now();
//
//		results.sizeOnDisk = Files.size(file);
//
//		results.readStart = Instant.now();
//		List<TestValue> deserializedValues = (List<TestValue>) deserializeObjectUsingObjectStream();
//		results.readEnd = Instant.now();
//
//		assertEquals(values, deserializedValues);
//		results.print("testAsListUsingObjectStream");
//	}

	private void assertEquals(List<TestValue> l1, List<TestValue> l2) {
		if(!l1.equals(l2)) {
			System.err.println("Lists are not equal: [l1]" + l1 + " [l2]" + l2);
		}
	}

	private void serializeListToFileOneObjectAtATime() throws FileNotFoundException, IOException {
		try(DataOutputStream dos = new DataOutputStream(new FileOutputStream(file.toFile()))) {
			for(TestValue value : values) {
				byte[] bytes = serializer.serializeObjectToByteArray(value);
				int len = bytes.length;

				dos.writeInt(len);
				dos.write(bytes);
				dos.flush();
			}
		}
	}

	private List<TestValue> deserializeListFromFileOneObjectAtATime() throws FileNotFoundException, IOException {
		List<TestValue> values = new LinkedList<>();
		
		try(DataInputStream dis = new DataInputStream(new FileInputStream(file.toFile()))) {
			try {
				while(true) {
					int len = dis.readInt();
					byte buffer[] = new byte[len];
					while (len > 0) {
						len -= dis.read(buffer, buffer.length - len, len);
					}

					values.add((TestValue) serializer.deserializeObjectFromByteArray(buffer));
				}
			} catch(EOFException e) {
			}
		}

		return values;
	}

//	private void serializeListToFileOneObjectAtATimeInObjectStream() throws FileNotFoundException, IOException {
//		try(FileOutputStream fos = new FileOutputStream(file.toFile())) {
//			ObjectOutput oos = serializer.getObjectOutputStream(fos);
//			for(TestValue value : values) {
//				oos.writeObject(value);
//				oos.flush();
//			}
//		}
//	}

//	private List<TestValue> deserializeListFromFileOneObjectAtATimeInObjectStream() throws FileNotFoundException, IOException, ClassNotFoundException {
//		List<TestValue> values = new LinkedList<>();
//		
//		try(FileInputStream fis = new FileInputStream(file.toFile())) {
//			ObjectInput ois = serializer.getObjectInputStream(fis);
//			
//			try {
//				while(true) {
//					values.add((TestValue) ois.readObject());
//				}
//			} catch(Throwable t) {
//			}
//		}
//
//		return values;
//	}

	private void serializeObject(Object obj) throws FileNotFoundException, IOException {
		byte[] bytes = serializer.serializeObjectToByteArray(obj);

		try(FileOutputStream fos = new FileOutputStream(file.toFile())) {
			fos.write(bytes);
			fos.flush();
		}
	}

	private Object deserializeObject() throws FileNotFoundException, IOException {
		byte[] bytes = Files.readAllBytes(file);
		return serializer.deserializeObjectFromByteArray(bytes);
	}

//	private void serializeObjectUsingObjectStream(Object obj) throws FileNotFoundException, IOException {
//		try(FileOutputStream fos = new FileOutputStream(file.toFile())) {
//			ObjectOutput oos = serializer.getObjectOutputStream(fos);
//			oos.writeObject(obj);
//			oos.flush();
//		}
//	}

//	private Object deserializeObjectUsingObjectStream() throws FileNotFoundException, IOException, ClassNotFoundException {
//		try(FileInputStream fis = new FileInputStream(file.toFile())) {
//			ObjectInput ois = serializer.getObjectInputStream(fis);
//			return ois.readObject();
//		}
//	}
	
	private static class TestResults {
		public long sizeOnDisk;
		public Instant writeStart;
		public Instant writeEnd;
		public Instant readStart;
		public Instant readEnd;
		
		public Duration getWriteDuration() {
			return Duration.between(writeStart, writeEnd);
		}

		public Duration getReadDuration() {
			return Duration.between(readStart, readEnd);
		}
		
		public void print(String title) {
			Duration writeDuration = getWriteDuration();
			Duration readDuration = getReadDuration();
			
			System.out.println(title);
			System.out.println("Size on Disk: " + sizeOnDisk);
			System.out.println("Write Time: " + writeDuration);
			System.out.println("Read Time: " + readDuration);
			System.out.println("Total Time: " + writeDuration.plus(readDuration));
			System.out.println("");
		}
	}
	
	public static void main(String[] args) throws Exception {
//		PerformanceTests tests = new PerformanceTests();
//		tests.setUp(true);
//		tests.testOneObjectAtATimeUsingObjectStream();
//		tests.tearDown();
		
		PerformanceTests tests = new PerformanceTests();
		tests.setUp(true);
		tests.testOneObjectAtATime();
		tests.tearDown();
		
//		PerformanceTests tests = new PerformanceTests();
//		tests.setUp(false);
//		tests.testAsList();
//		tests.tearDown();
		
//		PerformanceTests tests = new PerformanceTests();
//		tests.setUp(false);
//		tests.testAsArray();
//		tests.tearDown();
		
//		PerformanceTests tests = new PerformanceTests();
//		tests.setUp(false);
//		tests.testAsListUsingObjectStream();
//		tests.tearDown();
	}
}
