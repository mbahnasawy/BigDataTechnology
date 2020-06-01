package Q3;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Quiz {
	public static class AverageTemperatureMapper extends Mapper<LongWritable, Text, Text, Pair>
	{
		private Text year = new Text();
		
		HashMap<Text, Pair> hashmap ;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			
			hashmap = new HashMap<Text, Pair>();
				
		}
		 
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			
			for (String token : value.toString().split("\\s+"))
			{
				// Get fist letter to ubber case
				String upper = token.toUpperCase() ; 
				
				char letter = token.charAt(0);
				
				if(hashmap.containsKey(letter)){
					
					hashmap.get(letter).setSum(hashmap.get(letter).getSum() + upper.length());
					hashmap.get(year).setCount(hashmap.get(letter).getCount() + 1);
					
				}else{
					
					Pair p = new Pair(upper.length(), 1); 
					hashmap.put(new Text(letter+""), p);
					
				}
				
			}
		
	
			
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
				
			for(Text key: hashmap.keySet()){
				context.write(key, hashmap.get(key));
			}
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
			
		//	System.out.println("reducer " + key );
			
		}
	}
	

}
