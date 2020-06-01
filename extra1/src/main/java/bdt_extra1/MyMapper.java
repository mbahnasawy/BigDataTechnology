package bdt_extra1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, CompositeKey, IntWritable> {

	 
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		for (String token : value.toString().split("\\s+")) {
			String stationId = token.substring(4, 10);
			String temp =token.substring(87, 92);
			CompositeKey compositeKey = new CompositeKey(stationId, Integer.parseInt(temp));
			String year = token.substring(15, 19);	
			context.write(compositeKey, new IntWritable(Integer.parseInt(year)) );
		}
	}

}
