import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroMaxTempYear extends Configured implements Tool
{

	private static Schema SCHEMA;

	public static class AvroMapper extends Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable>
	{
		private NcdcLineReaderUtils utils = new NcdcLineReaderUtils();
		
		
		HashMap< Integer, Float> maxMap = new HashMap<Integer, Float>();
		HashMap< Integer,  AvroKey<GenericRecord>> recordMap = new HashMap<Integer,  AvroKey<GenericRecord>>();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			utils.parse(value.toString());

			if (utils.isValidTemperature())
			{
				GenericRecord record = new GenericData.Record(SCHEMA);
				record.put("stationId", utils.getStationId());
				record.put("temperature", utils.getAirTemperature());
				record.put("year", utils.getYearInt());
				
				//populate the avroKey with record
				AvroKey<GenericRecord> avroKey = new AvroKey<GenericRecord>(record);
				avroKey.datum(record);
				
				if(maxMap.containsKey(utils.getYearInt())) {
					if(maxMap.get(utils.getYearInt()) < utils.getAirTemperature()) {
						maxMap.put(utils.getYearInt(), utils.getAirTemperature()) ; // update the value
						recordMap.put(utils.getYearInt(), avroKey);
					}
					
				}else {
					maxMap.put(utils.getYearInt(), utils.getAirTemperature()) ; // update the value
					recordMap.put(utils.getYearInt(), avroKey);
				}
				
				
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
				
			for(Integer key: recordMap.keySet()){
				
				context.write(recordMap.get(key), NullWritable.get());
			}
		}
		
		
	}

	public static class AvroReducer extends Reducer<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable>
	{
		@Override
		protected void reduce(AvroKey<GenericRecord> key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException
		{
			context.write(key, NullWritable.get());
		}
	}

	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length != 3)
		{
			System.err.printf("Usage: %s [generic options] <input> <output> <schema-file>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = Job.getInstance();
		job.setJarByClass(AvroMaxTempYear.class);
		job.setJobName("Avro Station-Temp-Year");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		String schemaFile = args[2];

		SCHEMA = new Schema.Parser().parse(new File(schemaFile));

		job.setMapperClass(AvroMapper.class);
		job.setReducerClass(AvroReducer.class);
		
		job.setMapOutputValueClass(NullWritable.class);

		AvroJob.setMapOutputKeySchema(job, SCHEMA);
		AvroJob.setOutputKeySchema(job, SCHEMA);

		job.setOutputFormatClass(AvroKeyOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		FileSystem.get(conf).delete(new Path(args[1]), true);
		int res = ToolRunner.run(conf, new AvroMaxTempYear(), args);		
		System.exit(res);
	}
}