import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * Lab Extra Lab
 */
public class NCDCFormatter extends Configured implements Tool
{

	public static class NCDCFormatterMapper extends Mapper<LongWritable, Text, ComplexKey, Text>
	{
		private Text year = new Text();
		private ComplexKey myComplexKey = new ComplexKey();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String stationIDSubString = value.toString().substring(4, 15);
			String yearSubString = value.toString().substring(15, 19);
			int tempVal =  Integer.parseInt(value.toString().substring(87, 92)) ;
			
			year.set(yearSubString);
			myComplexKey.setStationID(stationIDSubString);;
			myComplexKey.setTemperature(tempVal);
			
			context.write(myComplexKey, year);
			
		}
		
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new NCDCFormatter(), args);

		FileSystem fs = FileSystem.get(new Configuration());
		 FileStatus fsStatus[] = fs.listStatus(new Path(args[1]));
		 
		 String pattern = "-r-00000" ; 
		 if (fsStatus != null){
			 for (FileStatus aFile : fsStatus) {
				 if (aFile.getPath().toString().endsWith(pattern)) {
					 String fileName = aFile.getPath().toString().replaceAll(pattern, "") ;
					 fs.rename(aFile.getPath(), new Path(fileName));
				 }
			 }
		 }
		 
		 
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{

		Job job = new Job(getConf(), "NCDCFormatter");
		job.setJarByClass(NCDCFormatter.class);

		job.setMapperClass(NCDCFormatterMapper.class);

		job.setOutputKeyClass(ComplexKey.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Q2
		job.getConfiguration().set("mapreduce.output.basename", "StationTempRecord");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[1]), true);
		 

		return job.waitForCompletion(true) ? 0 : 1;
	}
}