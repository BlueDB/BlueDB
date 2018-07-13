package io.bluedb.api.keys;

public class IntegerKey implements ValueKey {
	private static final long serialVersionUID = 1L;

	private int id;

	public IntegerKey(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}
	
	@Override
	public long getGroupingNumber() {
		return hashCode();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		IntegerKey other = (IntegerKey) obj;
		if (id != other.id) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "IntegerKey [key=" + id + "]";
	}

	@Override
	public int compareTo(BlueKey other) {
		if(other == null) {
			return -1;
		} else if(other instanceof IntegerKey) {
			return Integer.compare(id, ((IntegerKey)other).id);
		} else {
			return getClass().getSimpleName().compareTo(other.getClass().getSimpleName());
		}		
	}
}
