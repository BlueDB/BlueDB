package org.bluedb.api.encryption;

import java.nio.file.Path;
import java.util.Optional;

public class EncryptionUtils {
	
	public static final String EBF = "ebf";

	public static Optional<String> getEncryptionVersionKey(String filePath) {
		return Optional.ofNullable(filePath)
				.filter(f -> f.contains("." + EBF + "."))
				.map(f -> f.substring(filePath.lastIndexOf(".") + 1));
	}

	public static Optional<String> getEncryptionVersionKey(Path path) {
		return getEncryptionVersionKey(path.getFileName());
	}
}
