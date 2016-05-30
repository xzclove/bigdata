package com.ibeifeng.hdfs.io.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PaiWritable implements WritableComparable<PaiWritable> {

	/**
	 * user info * id * name
	 */
	private int id;
	private String name;

	public PaiWritable() {
	}

	public PaiWritable(int id, String name) {
		this.set(id, name);
	}

	public void set(int id, String name) {
		this.setId(id);
		this.setName(name);
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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
		PaiWritable other = (PaiWritable) obj;
		if (id != other.id)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return id + "\t" + name;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeUTF(name);
	}

	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
		this.name = in.readUTF();
	}

	public int compareTo(PaiWritable o) {
		// compare id
		int comp = Integer.valueOf(this.getId()).compareTo(
				Integer.valueOf(o.getId()));

		//
		if(0 != comp){
			return comp ;
		}
		
		// compare name
		return this.getName().compareTo(o.getName());
	}

}
