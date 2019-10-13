package com.bookpractice.book1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class BookMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final IntWritable ONE = new IntWritable(1);
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] SingleCountryData = line.split(",");
		
		Text bokauthor=new Text(SingleCountryData[1]);

		context.write(bokauthor, ONE);

	

	}
}
