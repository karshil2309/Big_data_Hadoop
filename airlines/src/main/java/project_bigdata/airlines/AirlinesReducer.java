package project_bigdata.airlines;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AirlinesReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

private IntWritable value = new IntWritable();

public void reduce(Text key, Iterator<IntWritable> values,
 OutputCollector<Text, IntWritable> output, Reporter reporter)
 throws IOException {

int totalFlights = 0;
int delayedFlights = 0;
//Delayed 0 for ontime or NA, 1 for delayed
Text newKey = new Text();
while (values.hasNext()) {
 int delayed = values.next().get();
//Increment the totalFlights by 1
 totalFlights = totalFlights + 1;
 // Calculate the number of delayed flights
 delayedFlights = delayedFlights + delayed;
}


newKey.set(key.toString() + "\t" );

value.set(totalFlights);
output.collect(newKey, value);
}

}