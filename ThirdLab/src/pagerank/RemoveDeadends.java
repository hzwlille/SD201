package pagerank;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RemoveDeadends {

	enum myCounters{ 
		NUMNODES;
	}

	static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		protected void map(LongWritable key, Text value, Context context) 
			{
			// TO DO
			System.out.println(value.toString());
			}
		}
	

	static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		protected void reduce(Text key, Iterable<Text> values, Context context){
			
			//TO DO
		}
}

	public static void job(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException{
		
		
		boolean existDeadends = true;
		
		/* You don't need to use or create other folders besides the two listed below.
		 * In the beginning, the initial graph is copied in the processedGraph. After this, the working directories are processedGraphPath and intermediaryResultPath.
		 * The final output should be in processedGraphPath. 
		 */
		
		FileUtils.copyDirectory(new File(conf.get("graphPath")), new File(conf.get("processedGraphPath")));
		String intermediaryDir = conf.get("intermediaryResultPath");
		String currentInput = conf.get("processedGraphPath");
		
		long nNodes = conf.getLong("numNodes", 0);
		
			
		while(existDeadends)
		{
			Job job = Job.getInstance(conf);
			job.setJobName("deadends job");
			/* TO DO : configure job and move in the best manner the output for each iteration
			 * you have to update the number of nodes in the graph after each iteration,
			 * use conf.setLong("numNodes", nNodes);
			*/
			existDeadends = false;
			
		}		
		// when you finished implementing delete this line
		System.out.println("Good");
		throw new UnsupportedOperationException("Implementation missing");
		
		
	}
	
}