package Q4;
import java.io.IOException;
import java.util.HashMap;

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
 * Lab 3 - Q4
 * Modify the above program by writing your own sorting routine so that the output file will show the latest year first. 
 * (Years should be in descending order)
 * Hint: Create a Custom Class by extending WritableComparable and override compareTo method. 
 * This custom object will be passed around from mapper.
 */
public class AverageTemperature extends Configured implements Tool
{

	public static class AverageTemperatureMapper extends Mapper<LongWritable, Text, YearWritable, PairWritable>
	{
		private Text year = new Text();
		
		HashMap<Text, PairWritable> hashmap ;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			
			hashmap = new HashMap<Text, PairWritable>();
				
		}
		 
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String yearSubString = value.toString().substring(15, 19);
			int tempVal =  Integer.parseInt(value.toString().substring(87, 92)) ;
			
			year.set(yearSubString);
			
			if(hashmap.containsKey(year)){
				
				hashmap.get(year).setSum(hashmap.get(year).getSum() + tempVal);
				hashmap.get(year).setCount(hashmap.get(year).getCount() + 1);
				
			}else{
				
				PairWritable p = new PairWritable(tempVal, 1); 
				hashmap.put(new Text(year), p);
				
			}
			
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			YearWritable year = new YearWritable();
			for(Text key: hashmap.keySet()){
				year.setYear(key.toString());
				context.write(year, hashmap.get(key));
			}
		}
		
	}
	
	
	public static class AverageTemperatureReducer extends Reducer<YearWritable, PairWritable, YearWritable, DoubleWritable>
	{
		private DoubleWritable resultValue = new DoubleWritable();
	
		@Override
		public void reduce(YearWritable key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			int count = 0 ;
			for (PairWritable val : values)
			{
				sum += val.getSum();
				count += val.getCount();
			}
			
			double avg = ((double)sum/count) ; 
			resultValue.set(avg/10.0); //degrees Celsius x 10; convert it back to real one.
		
			context.write(key, resultValue);
			
			
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

		Job job = new Job(getConf(), "AverageTemperature4");
		job.setJarByClass(AverageTemperature.class);

		job.setMapperClass(AverageTemperatureMapper.class);
		job.setReducerClass(AverageTemperatureReducer.class);
        
		job.setMapOutputKeyClass(YearWritable.class);
        job.setMapOutputValueClass(PairWritable.class);
        
        job.setOutputKeyClass(YearWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

       
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[1]), true);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
