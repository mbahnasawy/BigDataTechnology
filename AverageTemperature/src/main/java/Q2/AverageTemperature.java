package Q2;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * Lab 3 - Q2
 * Write a MapReduce java program with combiner (no in-mapper combining) to calculate the average temperature per year.
 */
public class AverageTemperature extends Configured implements Tool
{

	public static class AverageTemperatureMapper extends Mapper<LongWritable, Text, Text, Pair>
	{

		private Pair pair = new Pair();
		private Text year = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String yearSubString = value.toString().substring(15, 19);
			int tempVal =  Integer.parseInt(value.toString().substring(87, 92)) ;
			
			year.set(yearSubString);
			pair.setSum(tempVal);
			pair.setCount(1);
			
			context.write(year, pair);
			
		}
		
	}
	
	public static class AverageTemperatureCombiner extends Reducer<Text, Pair, Text, Pair>
	{
		private Pair result = new Pair();

		@Override
		public void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			int count = 0 ;
			for (Pair val : values)
			{
				sum += val.getSum();
				count += val.getCount();
			}

			result.setSum(sum);
			result.setCount(count);
			context.write(key, result);
			
		}
	}
	
	public static class AverageTemperatureReducer extends Reducer<Text, Pair, Text, DoubleWritable>
	{
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			int count = 0 ;
			for (Pair val : values)
			{
				sum += val.getSum();
				count += val.getCount();
			}
			
			double avg = ((double)sum/count) ; 
			result.set(avg/10.0); //degrees Celsius x 10; convert it back to real one.
			context.write(key, result);
			
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new AverageTemperature(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "AverageTemperature2");
		job.setJarByClass(AverageTemperature.class);

		job.setMapperClass(AverageTemperatureMapper.class);
		job.setCombinerClass(AverageTemperatureCombiner.class);
		job.setReducerClass(AverageTemperatureReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Pair.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[1]), true);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
