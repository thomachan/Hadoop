package com.radiant.cisms.hdfs.seq;

import org.apache.commons.lang.builder.*;
import org.apache.hadoop.io.*;

import java.io.*;

public class HInfoWritable implements WritableComparable<HInfoWritable>,
		Cloneable {
	String oid;
	String objId;
	long time;
	double value;

	public HInfoWritable() {
	}

	public HInfoWritable(String oid, String objId, long time, double value) {
		this.oid = oid;
		this.objId = objId;
		this.time = time;
		this.value = value;

	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, oid);
		WritableUtils.writeString(out, objId);
		out.writeLong(time);
		out.writeDouble(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		oid = WritableUtils.readString(in);
		objId = WritableUtils.readString(in);
		time = in.readLong();
		value = in.readDouble();
	}

	@Override
	public int compareTo(HInfoWritable passwd) {
		return CompareToBuilder.reflectionCompare(this, passwd);
	}

	public String getOid() {
		return oid;
	}

	public void setOid(String oid) {
		this.oid = oid;
	}

	public String getObjId() {
		return objId;
	}

	public void setObjId(String objId) {
		this.objId = objId;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}
	@Override
	public String toString() {
		return objId="_"+ oid +"_"+time;
	}
}