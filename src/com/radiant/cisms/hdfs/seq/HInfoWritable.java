package com.radiant.cisms.hdfs.seq;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class HInfoWritable implements WritableComparable<HInfoWritable>,
		Cloneable {
	String oid;
	long objId;
	long time;
	double value;
	ByteBuffer buff;
	 private static final Log LOG = LogFactory.getLog(" org.apache.hadoop.io");
	public HInfoWritable() {
		buff = ByteBuffer.allocate(1024 * 64);
	}

	public HInfoWritable(String oid, long objId, long time, double value) {
		this.oid = oid;
		this.objId = objId;
		this.time = time;
		this.value = value;

	}

	public HInfoWritable(int defaultBufferSize) {
		buff = ByteBuffer.allocate(defaultBufferSize);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, oid);
		out.writeLong(objId);
		out.writeLong(time);
		out.writeDouble(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		oid = WritableUtils.readString(in);
		objId = in.readLong();
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

	public long getObjId() {
		return objId;
	}

	public void setObjId(long objId) {
		this.objId = objId;
	}

	public ByteBuffer getBuff() {
		return buff;
	}

	public void setBuff(ByteBuffer buff) {
		this.buff = buff;
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
	public void put(byte buffer[], int startPosn, int appendLength){
		System.out.println(new String(buffer));
		buff.put(buffer, startPosn, appendLength);
	}
	public void read(int start,int end) throws IOException{
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(this.buff.array(),start,end));
		readFields(in);
		in.close();
	}

	public void read() throws IOException {
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(this.buff.array(),0,this.buff.position()));
		readFields(in);
		in.close();
	}
	@Override
	public String toString() {
		return objId+"_"+ oid +"_"+time;
	}

	public void clear() {
		if(this.buff != null){
			this.buff.clear();
		}
		
	}
	/*public static void main(String []a){
		ByteBuffer buff = null;
		buff.put(new byte[]{},0,10);
	}*/
}