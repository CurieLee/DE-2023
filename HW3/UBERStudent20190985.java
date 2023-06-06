import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.Serializable;
import java.util.*;
import java.time.*;

public final class UBERStudent20190985 
{

    public static void main(String[] args) throws Exception 
    {

        if (args.length < 2) 
        {
            System.err.println("Usage: UBER <in-file> <out-file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName("UBERStudent20190985")
            .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
        
        PairFunction<String, String, String> pf = new PairFunction<String, String, String>() 
        {
            public Tuple2<String, String> call(String s) 
            {
            	String line[] = s.split(",");
            	String id = line[0];
            	String date = line[1];
            	String vehicles = line[2];
            	String trips = line[3];
            	
            	String value = trips + "," + vehicles;
            	String key = "";
            	
            	StringTokenizer itr2 = new StringTokenizer(date,"/");
            	while (itr2.hasMoreTokens())
            	{
            		int month = Integer.parseInt(itr2.nextToken());
			int day = Integer.parseInt(itr2.nextToken());
			int year = Integer.parseInt(itr2.nextToken());
			
			LocalDate date_a = LocalDate.of(year,month,day);
			DayOfWeek dayOfWeek = date_a.getDayOfWeek();
			String result = dayOfWeek.getDisplayName(TextStyle.SHORT, Locale.US);
			String result_upper = result.toUpperCase();
			key = id + "," + result_upper;
            	}
            	
                return new Tuple2(key, value);
            }
        };
      
        JavaPairRDD<String, String> map1 = lines.mapToPair(pf);
       
        Function2<String, String, String> f2 = new Function2<String, String, String>() 
        {
        	public String call(String s1, String s2) 
          {
        		String[] line1 = s1.split(",");
        		String[] line2 = s2.split(",");

        		int trip = Integer.parseInt(line1[0]) + Integer.parseInt(line2[0]);
        		int vehicle = Integer.parseInt(line1[1]) + Integer.parseInt(line2[1]);
        		
        		String value = trip + "," + vehicle;
        		
        		return value; 		
        	}
        };
      
	JavaPairRDD<String, String> reduce1 = map1.reduceByKey(f2);
       
        reduce1.saveAsTextFile(args[1]);
        spark.stop();
    }
}
