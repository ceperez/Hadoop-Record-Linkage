package com.fluent.hadoop;

import java.io.IOException;

import static java.lang.System.exit;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.fluent.hadoop.Faster_Word_Similarity_Mapper.Entry_Array;

public class Word_Similarity extends Configured implements Tool
{
	@Override public int run(final String[] args) throws Exception
	{
		final Job job = job_from(Job_Builder.parseInputAndOutput(this, getConf(), args));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) throws Exception
	{
		exit(ToolRunner.run(new Word_Similarity(), args));
	}

	static Job job_from(final Job job) throws IOException
	{
		job.setJarByClass(Word_Similarity.class);
		job.setJobName("Word Similarity");
		job.setMapperClass(Faster_Word_Similarity_Mapper.class);
		job.setReducerClass(Faster_Word_Similarity_Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Entry.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Entry_Array.class);

		return job;
	}
}