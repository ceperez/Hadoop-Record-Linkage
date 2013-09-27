package com.fluent.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static com.fluent.hadoop.Similarity.Similarity;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class Word_Similarity_Mapper extends Mapper<LongWritable, Text, Text, ArrayWritable>
{
	List<String> uk_towns;

	@Override public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
	{
		final String us_town = parse_us_town_line(value.toString());
		final List<Writable> entries = new ArrayList<Writable>();

		for (final String uk_town : uk_towns)
		{
			entries.add(new Entry(uk_town, Similarity.normal_levenshtein(us_town, uk_town)));
		}

		context.write(new Text(us_town), new Entry_Array(entries));
	}

	@Override protected void setup(final Context context) throws IOException, InterruptedException
	{
		uk_towns = new ArrayList<String>();

		final Scanner scanner = new Scanner(new File("src/main/resources/in/metadata/places.csv"));

		while (scanner.hasNextLine())

			uk_towns.add(parse_uk_town_line(scanner.nextLine()));
	}

	static String parse_uk_town_line(final String line)
	{
		return line.split("\\,")[1].replaceAll("\"", "");
	}

	static String parse_us_town_line(final String line)
	{
		return line.split("\\|")[1].replaceAll("\\(historical\\)", "").trim();
	}

	static class Entry_Array extends ArrayWritable
	{
		public Entry_Array()
		{
			super(Entry.class);
		}

		public Entry_Array(final List<Writable> entries)
		{
			this();
			set(entries.toArray(new Writable[entries.size()]));
		}
	}
}