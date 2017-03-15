package org.mahedi.bigdata.mapreduce.reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;


public class MyWritableComparable implements WritableComparable<IntWritable> {
	// Some data
	private int value;
	private int counter;
	private long timestamp;

	public MyWritableComparable(int value) {
		// TODO Auto-generated constructor stub
		this.value = value;
	}
	public int get() {
		return value;
	}
	public void write(DataOutput out) throws IOException {
		out.writeInt(counter);
		out.writeLong(timestamp);
	}

	public void readFields(DataInput in) throws IOException {
		counter = in.readInt();
		timestamp = in.readLong();
	}

//	public int compareTo(MyWritableComparable o) {
//		int thisValue = this.value;
//		int thatValue = o.value;
//		return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
//	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + counter;
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

//	@Override
//	public int compareTo(Object arg0) {
//		MyWritableComparable o = (MyWritableComparable) arg0;
//		int thisValue = this.value;
//		int thatValue = o.value;
//		return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
//	}

	@Override
	public int compareTo(IntWritable o) {
		int thisValue = this.value;
		int thatValue = o.get();
		
		return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
	}
}
