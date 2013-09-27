package com.fluent.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**
 * 
 * Copied from https://github.com/tomwhite/hadoop-book/blob/master/common/src/main/java/JobBuilder.java
 * 
 */
public class Job_Builder
{
	private final Class<?> driverClass;
	private final Job job;
	private final int extraArgCount;
	private final String extrArgsUsage;
	private String[] extraArgs;

	public Job_Builder(final Class<?> driverClass) throws IOException
	{
		this(driverClass, 0, "");
	}

	public Job_Builder(final Class<?> driverClass, final int extraArgCount, final String extrArgsUsage) throws IOException
	{
		this.driverClass = driverClass;
		this.extraArgCount = extraArgCount;
		job = new Job();
		job.setJarByClass(driverClass);
		this.extrArgsUsage = extrArgsUsage;
	}

	public Job build()
	{
		return job;
	}

	public String[] getExtraArgs()
	{
		return extraArgs;
	}

	public Job_Builder withCommandLineArgs(final String... args) throws IOException
	{
		final Configuration conf = job.getConfiguration();
		final GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		final String[] otherArgs = parser.getRemainingArgs();
		if (otherArgs.length < 2 && otherArgs.length > 3 + extraArgCount)
		{
			System.err.printf("Usage: %s [genericOptions] [-overwrite] <input path> <output path> %s\n\n", driverClass.getSimpleName(),
					extrArgsUsage);
			GenericOptionsParser.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
		int index = 0;
		boolean overwrite = false;
		if (otherArgs[index].equals("-overwrite"))
		{
			overwrite = true;
			index++;
		}
		final Path input = new Path(otherArgs[index++]);
		final Path output = new Path(otherArgs[index++]);

		if (index < otherArgs.length)
		{
			extraArgs = new String[otherArgs.length - index];
			System.arraycopy(otherArgs, index, extraArgs, 0, otherArgs.length - index);
		}

		if (overwrite)
		{
			output.getFileSystem(conf).delete(output, true);
		}

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		return this;
	}

	public static Job parseInputAndOutput(final Tool tool, final Configuration conf, final String[] args) throws IOException
	{

		if (args.length != 2)
		{
			printUsage(tool, "<input> <output>");
			return null;
		}
		final Job job = new Job(conf);
		job.setJarByClass(tool.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job;
	}

	public static void printUsage(final Tool tool, final String extraArgsUsage)
	{
		System.err.printf("Usage: %s [genericOptions] %s\n\n", tool.getClass().getSimpleName(), extraArgsUsage);
		GenericOptionsParser.printGenericCommandUsage(System.err);
	}
}