import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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

public class WordCount extends Configured implements Tool {

	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			for (String token : value.toString().split("\\s+")) {
				word.set(token);
				context.write(word, one);

			}
		}

		public boolean isHadoopOrJava(String word) {
			return word.equalsIgnoreCase("java") || word.equalsIgnoreCase("hadoop");
		}
	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			result.set(sum);
			context.write(key, result);

		}
	}

	public static class WordCountDistinctReducer extends Reducer<Text, IntWritable, String, IntWritable> {
		int Distinct = 0;
		String myKey = "Distinct";

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			Distinct++;
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			context.write(myKey, new IntWritable(Distinct));
		}
	}

	public static class WordCountMapperPerLetter extends Mapper<LongWritable, Text, Text, Pair> {

		private Text word = new Text();

		protected void setup(Context context) throws IOException, InterruptedException {
			String s = "ABCDFGHIJKLMNOPQRSTWXYZ"; 
			
			for(char c: s.toCharArray()) {
				word.set(c + "");
				context.write(word,new Pair(0, 0));
			}
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			for (String token : value.toString().split("\\s+")) {
				token = token.toUpperCase();
				if(token.length()>0) {
					word.set(token.charAt(0) + "");
					context.write(word, new Pair(token.length(), 1));
				}
				

			}
		}
	}
	
	public static class WordCountcombinnerPerLetter extends Reducer<Text, Pair, Text, Pair>
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
	
	public static class WordCountReducerPerLetter extends Reducer<Text, Pair, Text, DoubleWritable>
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
			
			if(count != 0) {
				double avg = ((double)sum/count) ; 
				result.set(avg); 
				context.write(key, result);
			}else {
				result.set(0); 
				context.write(key, result);
			}

			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new WordCount(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "WordCount");
		job.setJarByClass(WordCount.class);

		job.setMapperClass(WordCountMapperPerLetter.class);
		job.setReducerClass(WordCountReducerPerLetter.class);
		job.setCombinerClass(WordCountcombinnerPerLetter.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Pair.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// - Part B
		/*
		 * Run the above basic Word Count program in pseudo distributed mode with 2
		 * reducers. Use setNumReduceTask method of Job object. Paste the screenshot of
		 * the two part- r-* files created in HDFS.
		 */
		// job.setNumReduceTasks(2);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// - Part A
		/*
		 * Do some research and find out what code needs to be added to the Word Count
		 * program for automatic removal of “output” directory before job execution.
		 */
		FileSystem fs = FileSystem.get(new Configuration());
		// true stands for recursively deleting the folder you gave
		fs.delete(new Path("output"), true);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
