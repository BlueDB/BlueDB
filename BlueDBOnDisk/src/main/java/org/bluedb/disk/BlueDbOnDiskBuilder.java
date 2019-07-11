package org.bluedb.disk;

import java.nio.file.Path;
import java.nio.file.Paths;

public class BlueDbOnDiskBuilder {
	private Path path = Paths.get(".", "bluedb");
	
	public BlueDbOnDiskBuilder withPath(Path path) {
		this.path = path;
		return this;
	}

	public BlueDbOnDisk build() {
		return new BlueDbOnDisk(path);
	}
	
	@Deprecated
	public BlueDbOnDiskBuilder setPath(Path path) {
		this.path = path;
		return this;
	}
}
