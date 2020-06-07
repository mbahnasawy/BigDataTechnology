package cs523;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.regex.*;
import scala.Tuple2;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SparkApacheLog
{

	/** The number of fields that must be found. */
	public static final int NUM_FIELDS = 9;
	public static final String LOG_PATTERN = "^(\\S+) (\\S+) (\\S+) " +  
             "\\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+)" +  
             " (\\S+)\\s*(\\S+)?\\s*\" (\\d{3}) (\\S+)"; 
	 
	public static final String logEntryLine = "67.131.107.5 - - [12/Mar/2004:11:39:31 -0800] \"GET /twiki/pub/TWiki/TWikiLogos/twikiRobot46x50.gif HTTP/1.1\" 200 2877" ;
	
	public static void listIPsThatAccessMoreThan(String logFolder, int threshold, String outputFolder) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("ApacheLog").setMaster("local"));

		// Load log file
		JavaRDD<String> lines = sc.textFile(logFolder);
		
		JavaPairRDD<String, Integer> ips= lines
				.filter(line -> isLogEntry(line))
				.map(l -> getIP(l))
				.mapToPair(ip -> new Tuple2<String, Integer>(ip, 1))
				.reduceByKey((x, y) -> x + y)
				.filter(t -> t._2 >= threshold);
		
		
		ips.saveAsTextFile(outputFolder);
		
		sc.close();

	}
			
	public static void howManyErrorsBetween(String logFolder,String error, String from, String to, String outputFolder) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("ApacheLog").setMaster("local"));

		// Load log file
		JavaRDD<String> lines = sc.textFile(logFolder);
		
		JavaPairRDD<String, Integer> result = lines
				.filter(line -> isMatch(line, error, from, to))
				.mapToPair(l -> new Tuple2<String, Integer>("All", 1))
				.reduceByKey((x,y) -> x+y);
		
		Long count = lines
				.filter(line -> isMatch(line, error, from, to))
				.count();
		
		System.out.println(count);
		
		result.saveAsTextFile(outputFolder);
		
		sc.close();
		
		
	}
	
	private static boolean isMatch(String log, String error, String from, String to) {

		 Pattern p = Pattern.compile(LOG_PATTERN);
		 Matcher matcher = p.matcher(log);
		 if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
			     return false; 
		}else {
		//	System.out.println("Error: " + matcher.group(8));
			//System.out.println("Date: "  + matcher.group(4).substring(0, 11));
			
			if(matcher.group(8).equals(error) && isInRange(matcher.group(4).substring(0, 11), from, to)) {
				return true;
			}
		}
		 
		
		return false;
	}
	
	private static boolean isLogEntry(String log) {

		 Pattern p = Pattern.compile(LOG_PATTERN);
		 Matcher matcher = p.matcher(log);
		 if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
			     return false; 
		}
		 
		return true;
	}
	
	private static String getIP(String log) {
		Pattern p = Pattern.compile(LOG_PATTERN);
		Matcher matcher = p.matcher(log);
		matcher.matches();
		return matcher.group(1); 
	}
	
	private static boolean isInRange(String target, String from, String to ) {
		DateFormat df = new SimpleDateFormat("dd/MMM/yyyy");
		
		//System.out.println("In Range call");
	     try
	       {
	           //format() method Formats a Date into a date/time string. 
	           Date fromDate = df.parse(from);
	           Date toDate = df.parse(to);
	           Date targetDate = df.parse(target);
	           
	          // System.out.println("From: "+targetDate.compareTo(fromDate) + " To: " + targetDate.compareTo(toDate));
	           
	           if(targetDate.compareTo(fromDate)>=0  && targetDate.compareTo(toDate)<=0 ){
	        	   //System.out.println("In");
	        	   return true;
	           }
	

	       }
	       catch (Exception ex ){
	    	  // System.out.println("Catch");
	          return false;
	       }
	     
	     return false; 
	}

	public static void main(String[] args) throws Exception
	{
		String inputFolder = args[0] ;
		String outputFolder1 = args[1];
		String outputFolder2 = args[2];
		
		String error = args[3];
		String from = args[4];
		String to = args[5];
		
		int threshold = Integer.parseInt(args[6]);
		
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(outputFolder1), true);
		fs.delete(new Path(outputFolder2), true);
		
		// Q1 - How many 401 errors from 07/Mar/2004 to 10/Mar/2004
		howManyErrorsBetween(inputFolder, error, from, to, outputFolder1);
		
		// Q2- list all IPs that access this server more than 20 times
		listIPsThatAccessMoreThan(inputFolder, threshold, outputFolder2);
		

		/*
		 Pattern p = Pattern.compile(LOG_PATTERN);
		 Matcher matcher = p.matcher(logEntryLine);
		
		    System.out.println("Match count: " + matcher.groupCount());
		if (!matcher.matches() || 
			      NUM_FIELDS != matcher.groupCount()) {
			      System.err.println("Bad log entry (or problem with RE?):");
			      System.err.println(logEntryLine);
			      return;
			    }
			    System.out.println("IP Address: " + matcher.group(1));
			    System.out.println("Date&Time: " + matcher.group(4));
			    System.out.println("Request: " + matcher.group(5));
			    System.out.println("Response: " + matcher.group(6));
			    System.out.println("Bytes Sent: " + matcher.group(7));
			    if (!matcher.group(8).equals("-"))
			      System.out.println("Referer: " + matcher.group(8));
			    System.out.println("Browser: " + matcher.group(9));
	
		*/
	}
}
