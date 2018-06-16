package io.bluedb.disk;

import java.io.FileOutputStream;
import java.io.IOException;
import org.nustaq.serialization.FSTConfiguration;
import io.bluedb.api.exceptions.BlueDbException;

public class Blutils {
	private static final FSTConfiguration serializer = FSTConfiguration.createDefaultConfiguration();

	public static void save(String path, Object o) throws BlueDbException {
		try (FileOutputStream fos = new FileOutputStream(path)) {
			byte[] bytes = serializer.asByteArray(o);
			fos.write(bytes);
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new BlueDbException("error writing to disk (" + path +")", e);
		}
	}
}
