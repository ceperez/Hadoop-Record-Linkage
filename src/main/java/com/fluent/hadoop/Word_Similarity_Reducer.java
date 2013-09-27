package com.fluent.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class Word_Similarity_Reducer extends Reducer<Text, ArrayWritable, Text, Entry>
{
	public void reduce(final Text us_town, final Iterable<ArrayWritable> values, final Context context) throws IOException,
			InterruptedException
	{
		double max_similarity = Double.MIN_VALUE;
		String max_uk_town = "";
		final ArrayWritable entries = values.iterator().next();

		for (final Writable writable : entries.get())
		{
			final Entry entry = (Entry) writable;

			if (entry.second.get() > max_similarity)
			{
				max_similarity = entry.second.get();
				max_uk_town = entry.first.toString();
			}
		}

		context.write(us_town, new Entry(max_uk_town, max_similarity));
	}
}