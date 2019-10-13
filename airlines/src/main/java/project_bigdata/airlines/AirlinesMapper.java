package project_bigdata.airlines;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
 
public class AirlinesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
 
 public void map(LongWritable key, Text value,OutputCollector<Text, IntWritable> output, Reporter reporter)
   throws IOException {
 
	 //Spliting the lines
    String[] pieces = value.toString().split(",");
 
 // Delayed 0 for ontime or NA, 1 for delayed
    int delayed = 0;
 // Get the origin which is the 17 field in the input line
   String origin = pieces[16];
 
   // 5 DepTime actual departure time (local, hhmm)
   // 6 CRSDepTime scheduled departure time (local, hhmm)
  if (StringUtils.isNumeric(pieces[4])
    && StringUtils.isNumeric(pieces[5])) {
 	int actualDepTime = Integer.parseInt(pieces[4]);
   int scheduledDepTime = Integer.parseInt(pieces[5]);
 
   int delay_By15min = 0;		
    if (actualDepTime > scheduledDepTime) {
	delay_By15min = actualDepTime - scheduledDepTime;	
	// Send the Origin and the delayed status to the reducer for aggregation
	  // ex., (ORD, 1)	
	if(delay_By15min > 15){    
		delayed = 1;
	}
   }
   
    
  }
  output.collect(new Text(origin), new IntWritable(delayed));
  
 }
}