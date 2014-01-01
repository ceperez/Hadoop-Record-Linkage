package com.fluent.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class Word_Similarity_Reducer extends Reducer<Text, ArrayWritable, Text, Entry>
{
	public void reduce(final Text us_town, final Iterable<ArrayWritable> entries, final Context context) throws IOException, InterruptedException
	{
		context.write(us_town, (Entry)entries.iterator().next().get()[0]);
	}
}