package project_bigdata.airlines;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
 
public class airlinesDriver {
 
 public static void main(String[] args) throws IOException {
 
	// Create the JobConf instance and specify the job name
  JobConf conf = new JobConf(airlinesDriver.class);
  conf.setJobName("Airline Delay");
  
//First and second arguments are input and output folder
  FileInputFormat.addInputPath(conf, new Path(args[0]));
  FileOutputFormat.setOutputPath(conf, new Path(args[1]));
  
//Specify the mapper and the reducer class
  conf.setMapperClass(AirlinesMapper.class);
  conf.setReducerClass(AirlinesReducer.class);
//Specify the output key and value of the entire job
  conf.setOutputKeyClass(Text.class);
  conf.setOutputValueClass(IntWritable.class);
//Specify the number of reducers tasks to run at any instant on a
 // machine, defaults to one
  conf.setNumReduceTasks(4);
  
//Trigger the mapreduce program
  JobClient.runJob(conf);
 }
}