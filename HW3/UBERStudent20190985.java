import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.Serializable;
import java.util.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.TextStyle;

public final class UBERStudent20190985 implements Serializable 
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
				StringTokenizer itr = new StringTokenizer(s, ",");

				String number = itr.nextToken().trim();
				String date = itr.nextToken().trim();
				
				int year = Integer.parseInt(date.split("/")[2]);
				int month = Integer.parseInt(date.split("/")[0]);
				int day = Integer.parseInt(date.split("/")[1]);
				
				LocalDate d = LocalDate.of(year, month, day);
				DayOfWeek dayOfWeek = d.getDayOfWeek();
				
				String dayEng=dayOfWeek.getDisplayName(TextStyle.SHORT,Locale.US).toUpperCase();;
				if (date.equals("THU")) date = "THR";
					    
				String numDay = number + "," + dayEng;
			    	
			    	String vehicles = itr.nextToken().trim();
			    	String trips = itr.nextToken().trim();
			    	String vehtrip = trips + "," + vehicles;
			    	
				return new Tuple2(numDay, vehtrip);
			}
		};
		
		JavaPairRDD<String, String> pair = lines.mapToPair(pf);

        Function2<String, String, String> f2 = new Function2<String, String, String>() 
	{
            public String call(String x, String y) 
	    {
            	String[] vt1 = x.split(",");
            	String[] vt2 = y.split(",");
            	
            	int trips = Integer.parseInt(vt1[0]) + Integer.parseInt(vt2[0]);
            	int vehicles = Integer.parseInt(vt1[1]) + Integer.parseInt(vt2[1]);
            
                return trips + "," + vehicles;
            }
        };
	
        JavaPairRDD<String, String> sum = pair.reduceByKey(f2);
        
        JavaRDD<String> result = sum.map(x -> x._1 + " " + x._2);

        result.saveAsTextFile(args[1]);
        spark.stop();
    }
}
