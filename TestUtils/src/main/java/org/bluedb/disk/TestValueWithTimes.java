package org.bluedb.disk;

import java.io.Serializable;

public class TestValueWithTimes implements Serializable {
	private static final long serialVersionUID = 1L;

	private long id;
	private long start;
	private long end;
	private String name;
	private int cupcakes = 0;

	public TestValueWithTimes(long id, long start, long end, String name) {
		this.id = id;
		this.start = start;
		this.end = end;
		this.name = name;
	}

	public TestValueWithTimes(long id, long start, long end, String name, int cupcakes) {
		this.id = id;
		this.start = start;
		this.end = end;
		this.name = name;
		this.cupcakes = cupcakes;
	}
	
	public long getId() {
		return id;
	}
	
	public long getStart() {
		return start;
	}
	
	public long getEnd() {
		return end;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getCupcakes() {
		return cupcakes;
	}

	public void addCupcake() {
		cupcakes += 1;
	}

	public void setCupcakes(int cupcakes) {
		this.cupcakes = cupcakes;
	}

	public void doSomethingNaughty() {
		throw new RuntimeException("you shall not pass!");
	}
	
	public TestValueWithTimes clone() {
		return new TestValueWithTimes(this.id, this.start, this.end, this.name, this.cupcakes);
	}
	
	public TestValueWithTimes cloneWithNewCupcakeCount(int cupcakes) {
		return new TestValueWithTimes(this.id, this.start, this.end, this.name, cupcakes);
	}
	
	@Override
	public String toString() {
		return "TestValueWithTimes [id=" + id + ", start=" + start + ", end=" + end + ", name=" + name + ", cupcakes="
				+ cupcakes + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + cupcakes;
		result = prime * result + (int) (end ^ (end >>> 32));
		result = prime * result + (int) (id ^ (id >>> 32));
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + (int) (start ^ (start >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TestValueWithTimes other = (TestValueWithTimes) obj;
		if (cupcakes != other.cupcakes)
			return false;
		if (end != other.end)
			return false;
		if (id != other.id)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (start != other.start)
			return false;
		return true;
	}
	
}
