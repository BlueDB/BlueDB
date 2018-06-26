package io.bluedb.disk;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import io.bluedb.api.Condition;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.serialization.BlueSerializer;

public class Blutils {
	public static void save(String path, Object o, BlueSerializer serializer) throws BlueDbException {
		byte[] bytes = serializer.serializeObjectToByteArray(o);
		Paths.get(path).toFile().getParentFile().mkdirs();
		try (FileOutputStream fos = new FileOutputStream(path)) {
			fos.write(bytes);
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
			// TODO delete the file
			throw new BlueDbException("error writing to disk (" + path +")", e);
		}
	}

	public static List<File> listFiles(Path path, String suffix) {
		File folder = path.toFile();
		if (!folder.exists()) {
			return new ArrayList<>();
		}
		File[] filesInFolder = folder.listFiles();
		List<File> results = new ArrayList<>();
		for (File file: filesInFolder) {
			if(file.getName().endsWith(suffix)) {
				results.add(file);
			}
		}
		return results;
	}

	public static <X extends Serializable> boolean meetsConditions(List<Condition<X>> conditions, X object) {
		for (Condition<X> condition: conditions) {
			if (!condition.test(object)) {
				return false;
			}
		}
		return true;
	}

	public static boolean meetsTimeConstraint(BlueKey key, long minTime, long maxTime) {
		if (key instanceof TimeFrameKey) {
			TimeFrameKey timeKey = (TimeFrameKey) key;
			return timeKey.getEndTime() >= minTime && timeKey.getStartTime() <= maxTime;
		}
		if (key instanceof TimeKey) {
			TimeKey timeKey = (TimeKey) key;
			return timeKey.getTime() >= minTime && timeKey.getTime() <= maxTime;
		}
		return true;
	}
}
