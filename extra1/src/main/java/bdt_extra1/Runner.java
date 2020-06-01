package bdt_extra1;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class Runner extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new Runner(), args);

		System.exit(res);
	}

//	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "Runner");
		job.setJarByClass(Runner.class);

		job.getConfiguration().set("mapreduce.output.basename", "text");
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(CompositeKey.class);
		job.setOutputValueClass(IntWritable.class);

		
		job.setInputFormatClass(TextInputFormat.class);
		
		MultipleOutputs.addNamedOutput(job, "StationTempRecord", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.setCountersEnabled(job, true);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);


		Path ouputPath = new Path(args[1]);
		File file = new File(ouputPath.toString());

		FileUtils.deleteQuietly(file);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, ouputPath);


		return job.waitForCompletion(true) ? 0 : 1;
	}
}
