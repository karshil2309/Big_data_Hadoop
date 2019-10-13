package com.bookpractice.book2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class BookDriver extends Configured implements Tool {

	public int run(String[] allArgs) throws Exception {

		Configuration conf = getConf();
		 
		Job job = Job.getInstance(conf, "Book Count");

		job.setJarByClass(com.bookpractice.book1.BookDriver.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(BookMapper.class);
		job.setReducerClass(BookMapReduce.class);
		job.setNumReduceTasks(1);

		String[] args = new GenericOptionsParser(conf, allArgs).getRemainingArgs();

		Path inputPath = new Path(args[0]);
		FileInputFormat.setInputPaths(job, inputPath);

		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);

		FileSystem.get(conf).delete(outputPath, true);

		boolean status = job.waitForCompletion(true);

		printCounterGroup(job);

		if (status) {
			return 0;
		} else {
			return 1;
		}

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new BookDriver(), args);
		System.exit(exitCode);
	}

	private void printCounterGroup(Job job) throws IOException {
		Iterator<String> groupNames = job.getCounters().getGroupNames().iterator();
		while (groupNames.hasNext()) {
			String groupName = groupNames.next();
			CounterGroup cntrGrp = job.getCounters().getGroup(groupName);
			Iterator<Counter> cntIter = cntrGrp.iterator();
			System.out.println("\nGroup Name = " + groupName);
			while (cntIter.hasNext()) {
				Counter cnt = cntIter.next();
				System.out.println(cnt.getName() + "=" + cnt.getValue());
			}
		}
	}
}
