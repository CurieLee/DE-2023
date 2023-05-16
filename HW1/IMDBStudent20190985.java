import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class IMDBStudent20190985
{
    	public static class IMDBMapper extends Mapper<Object, Text, Text, IntWritable>
    	{
		private final static IntWritable one = new IntWritable(1);
		private Text genre = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] splt = value.toString().split("::");
			StringTokenizer itr = new StringTokenizer(splt[splt.length - 1], "|");
			
			while (itr.hasMoreTokens()) 
			{
				genre.set(itr.nextToken());
				context.write(genre, one);
			}
		}
		
	}
	
	public static class IMDBReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable val : values)
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: IMDB <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "IMDBStudent20190985");
		
		job.setJarByClass(IMDBStudent20190985.class);
		job.setMapperClass(IMDBMapper.class);
		job.setReducerClass(IMDBReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
}
