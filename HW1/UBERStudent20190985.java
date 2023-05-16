
import java.io.IOException;
import java.util.*;
import java.time.*;
import java.time.format.TextStyle;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20190985
{

	public static class UBERMapper extends Mapper<Object, Text, Text, Text>
	{
		private Text map_key = new Text();
		private Text map_value = new Text();
		
		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			

			StringTokenizer itr = new StringTokenizer(value.toString(),",");
			while (itr.hasMoreTokens())
			{
				String id = itr.nextToken();
				String date = itr.nextToken();
				String vehicles = itr.nextToken();
				String trips = itr.nextToken();
				
				String value_value = trips + "," + vehicles;
				map_value.set(value_value);
				
				StringTokenizer itr2 = new StringTokenizer(date,"/");
				while (itr2.hasMoreTokens())
				{
					int month = Integer.parseInt(itr2.nextToken());
					int day = Integer.parseInt(itr2.nextToken());
					int year = Integer.parseInt(itr2.nextToken());
					
					LocalDate date_a = LocalDate.of(year,month,day);
					DayOfWeek dayOfWeek = date_a.getDayOfWeek();
					String result = dayOfWeek.getDisplayName(TextStyle.SHORT, Locale.US);
					if (result.equals("Thu"))
					{
						result = "Thr";
					} 
					
					String key_value = id + "," + result;
						
					map_key.set(key_value);
					
					
				}
				context.write(map_key, map_value);
				
				
			}
			
		}
	}

	public static class UBERReducer extends Reducer<Text,Text,Text,Text> 
	{

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			Text reduce_key = new Text();
			Text reduce_value = new Text();
			int total_trip = 0;
			int total_vehicle = 0;
			
			StringTokenizer itr = new StringTokenizer(key.toString(), ",");
			while(itr.hasMoreTokens())
			{
				String id = itr.nextToken();
				String day = itr.nextToken();
				
				String upper_day = day.toUpperCase();
				
				String result_value = id + "," + upper_day;
				
				reduce_key.set(result_value);
				
			}
			
			for (Text val: values)
			{
				
				StringTokenizer itr2 = new StringTokenizer(val.toString(), ",");
				while(itr2.hasMoreTokens())
				{
					int trips = Integer.parseInt(itr2.nextToken());
					int vehicles = Integer.parseInt(itr2.nextToken());
					total_trip += trips;
					total_vehicle += vehicles;
				}
			}
			String value_value = Integer.toString(total_trip) + "," + Integer.toString(total_vehicle);
			
			reduce_value.set(value_value);
			context.write(reduce_key, reduce_value);
			
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: UBER <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "UBER");
		
		job.setJarByClass(UBERStudent20190985.class);
		job.setMapperClass(UBERMapper.class);
		job.setReducerClass(UBERReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
	
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
