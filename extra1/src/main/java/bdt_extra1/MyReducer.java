package bdt_extra1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MyReducer extends Reducer<CompositeKey, IntWritable, Text, IntWritable> {
	MultipleOutputs<Text, IntWritable> mos;


	@Override
	public void setup(Context context) {
		mos = new MultipleOutputs(context);
	}

	@Override
	public void reduce(CompositeKey key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		Text outputKey = new Text();
		outputKey.set(String.format("%s %d", key.stationId, key.temp));
		for (IntWritable val : values) {
			mos.write("StationTempRecord",outputKey, val);
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}
