package pagerank;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
			{
			StringTokenizer itr=new StringTokenizer(value.toString(),"\t");
			Text a= new Text(itr.nextToken());

			Text b= new Text(itr.nextToken());

			Text a1=new Text("0 "+a);
			Text b1=new Text("1 "+b);
			context.write(a, b1);
			context.write(b, a1);
			
			}
		}
	

	static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			
			Text b = null;
			boolean hasSortant=false;
			Iterator<Text> myItr=values.iterator();
			ArrayList<Text> myText=new ArrayList<Text>();
			while(myItr.hasNext()){

				StringTokenizer itr=new StringTokenizer(myItr.next().toString()," ");
				IntWritable a= new IntWritable(Integer.parseInt(itr.nextToken()));
				b= new Text(itr.nextToken());

				if(a.get()==1){
					hasSortant=true;
				}
				else{
					myText.add(b);
				}
			}
			if(hasSortant){
				
				for(int i=0;i<myText.size();i++){
					context.write(myText.get(i), key );
					
				}
				Counter c=context.getCounter(myCounters.NUMNODES);
				c.increment(1);
			}
			
			
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
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.setInputPaths(job, new Path(conf.get("processedGraphPath")));
			FileOutputFormat.setOutputPath(job, new Path(conf.get("intermediaryResultPath")));
			job.waitForCompletion(true);

			
			FileUtils.deleteDirectory(new File(conf.get("processedGraphPath")));
		
			FileUtils.copyDirectory(new File(conf.get("intermediaryResultPath")), new File(conf.get("processedGraphPath")));
			FileUtils.deleteDirectory(new File(conf.get("intermediaryResultPath")));
			
			
			
			Counters counters=job.getCounters();
			Counter c1 = counters.findCounter(myCounters.NUMNODES);
			
			if(c1.getValue()==nNodes)
			{
				existDeadends = false;
			}
			/* TO DO : configure job and move in the best manner the output for each iteration
			 * you have to update the number of nodes in the graph after each iteration,
			 * use conf.setLong("numNodes", nNodes);
			*/
			else{
				conf.setLong("numNodes", c1.getValue());
				nNodes=conf.getLong("numNodes", c1.getValue());
			}
			
			
		}		
		
		
	}
	
}