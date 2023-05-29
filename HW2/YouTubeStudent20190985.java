import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class YouTubeStudent20190985
{
	public static class Youtube 
	{
		String cate;
		double avg;
		public Youtube(String cate, double avg) 
		{
			this.cate = cate;
			this.avg = avg;
		}
	}

	public static void insertYoutube(PriorityQueue q, String cate, double avg, int topK) 
	{
		Youtube youtube_head = (Youtube) q.peek();
		if ( q.size() < topK || youtube_head.avg < avg ) 
		{
			Youtube y = new Youtube(cate, avg);
			q.add(y);
			
			if( q.size() > topK ) 
			{
				q.remove();
			}
		}
	}

	public static class YoutubeComparator implements Comparator<Youtube> 
	{
		public int compare(Youtube x, Youtube y) 
		{
			if ( x.avg > y.avg ) 
			{
			       	return 1;
			}
			if ( x.avg < y.avg ) 
			{
			       	return -1;
			}
			return 0;
		}
	}
	
	public static class YoutubeMapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			Text outputKey = new Text();
			Text outputValue = new Text();
			String cate = "";
			String avg = "";
			String a = "";
			String b = "";
			
			StringTokenizer itr2 = new StringTokenizer(value.toString(), "|");
			while (itr2.hasMoreTokens()) 
			{
				a = itr2.nextToken();
				a = itr2.nextToken();
				a = itr2.nextToken();
				
				cate = itr2.nextToken();
				
				b = itr2.nextToken();
				b = itr2.nextToken();
				
				avg = itr2.nextToken();
			}
			
			outputKey.set(cate);
			outputValue.set(avg);
			
			context.write(outputKey, outputValue);
		}	
	}
	
	public static class YoutubeReducer extends Reducer<Text,Text,Text,DoubleWritable>
	{
		private PriorityQueue<Youtube> queue ;
		private Comparator<Youtube> comp = new YoutubeComparator();
		private int topK;
		
		Text reduce_key = new Text();
		DoubleWritable reduce_result = new DoubleWritable();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			double sum = 0;
			int count = 0;
			double avg = 0;
			String cate = key.toString();

			for (Text val : values) 
			{
				sum += Double.parseDouble(val.toString());
				count++;
			} 

			avg = sum / count;
			insertYoutube(queue, cate, avg, topK);
			
		}

		protected void setup(Context context) throws IOException, InterruptedException 
		{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("TopK", -1);
			queue = new PriorityQueue<Youtube>( topK , comp);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException 
		{
			while( queue.size() != 0 ) 
			{
				Youtube y = (Youtube) queue.remove();
				
				reduce_key.set(y.cate);
				reduce_result.set(y.avg);
				
				context.write(reduce_key, reduce_result);
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 3)
		{
			System.err.println("Usage: YouTube <in> <out> <topK>");
			System.exit(2);
		}
		
		conf.setInt("TopK", Integer.parseInt(otherArgs[2]));
		
		Job job = new Job(conf, "YouTube");
	
		job.setJarByClass(YouTubeStudent20190985.class);
		job.setMapperClass(YoutubeMapper.class);
		job.setReducerClass(YoutubeReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);	

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
