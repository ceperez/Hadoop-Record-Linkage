package com.fluent.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Entry implements WritableComparable<Entry>
{
	Text first;
	DoubleWritable second;

	public Entry()
	{
		this("", 0.);
	}

	public Entry(final String first, final double second)
	{
		set(new Text(first), new DoubleWritable(second));
	}

	@Override public int compareTo(final Entry other)
	{
		final int result = first.compareTo(other.first);

		return result != 0 ? result : second.compareTo(other.second);
	}

	@Override public boolean equals(final Object o)
	{
		return o == this || o instanceof Entry && first.equals(((Entry) o).first) && second.equals(((Entry) o).second);
	}

	public Text getFirst()
	{
		return first;
	}

	public DoubleWritable getSecond()
	{
		return second;
	}

	@Override public int hashCode()
	{
		return first.hashCode() * 163 + second.hashCode();
	}

	@Override public void readFields(final DataInput in) throws IOException
	{
		first.readFields(in);
		second.readFields(in);
	}

	public void set(final Text first, final DoubleWritable second)
	{
		this.first = first;
		this.second = second;
	}

	@Override public String toString()
	{
		return first + "\t" + second;
	}

	@Override public void write(final DataOutput out) throws IOException
	{
		first.write(out);
		second.write(out);
	}

}