package com.fluent.hadoop;

import static java.util.Arrays.*;

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
		double max_similarity = Double.MIN_VALUE;
		String max_uk_town = "";

		for (final String uk_town : uk_towns)
		{
			final double similarity = Similarity.normal_levenshtein(us_town, uk_town);

			if (similarity > max_similarity)
			{
				max_similarity = similarity;
				max_uk_town = uk_town;
			}
		}

		context.write(new Text(us_town),new Entry_Array(asList((Writable)new Entry(max_uk_town, max_similarity))));
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