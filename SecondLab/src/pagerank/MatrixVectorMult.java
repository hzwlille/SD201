package pagerank;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * Write the map and reduce functions for each job. For the second job write also the reduce function for the combiner.
 * To test your code run PublicTests.java.
 * On the site submit a zip archive of your src folder.
 * Try also the release tests after your submission. You have 3 trials per hour for the release tests. 
 * A correct implementation will get the same number of points for both public and release tests.
 * Please take the time also to understand the settings for a job, in the next lab your will need to configure it by yourself. 
 */

public class MatrixVectorMult {

	static class FirstMap extends Mapper<LongWritable, Text, IntWritable, Text> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			StringTokenizer itr=new StringTokenizer(value.toString()," ");
			String a= new String(itr.nextToken());
			String b= new String((itr.nextToken()));

			IntWritable theKey;
			Text theText;

			if(itr.hasMoreTokens()){

				theKey=new IntWritable(Integer.parseInt(b));
				String c=new String(itr.nextToken());
				theText= new Text(a.toString()+" "+c.toString());
			}
			else{
				theKey=new IntWritable(Integer.parseInt(a));
				theText= new Text(b.toString());
			}
			//	System.out.print(theKey);
			//	System.out.print("==============");
			//	System.out.println(theText);
			context.write(theKey, theText);
		}
	}


	static class FirstReduce extends Reducer<IntWritable, Text, IntWritable, DoubleWritable> {

		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

			StringTokenizer itr;
			ArrayList<String> element= new ArrayList<String>();
			Double valueToMultiply = null;
			Iterator<Text> count = values.iterator();
			int i = 0;
			IntWritable a;
			DoubleWritable b;
			Double b1;
			while(count.hasNext()){
				element.add(count.next().toString());
				itr=new StringTokenizer(element.get(i));			
				i++;
				if(itr.countTokens()!=2){
					valueToMultiply=Double.parseDouble(itr.nextToken());
				}

			}
			for(int j=0;j<i;j++){
				itr=new StringTokenizer(element.get(j));
				if(itr.countTokens()==2){
					a=new IntWritable(Integer.parseInt(itr.nextToken()));
					b1=Double.parseDouble(itr.nextToken());
					b=new DoubleWritable(b1*valueToMultiply);

					context.write(a,b);

					
				}
			}




		}
	}

	static class SecondMap extends Mapper<Text, Text, IntWritable, DoubleWritable> {

		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException
		{
			context.write(new IntWritable(Integer.parseInt(key.toString())), new DoubleWritable(Double.parseDouble(value.toString())));

		}
	}

	static class CombinerForSecondMap extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

		protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			System.out.println("!!!!!!!!!!!!!!!");
			Double calcule=(double) 0;
			Iterator<DoubleWritable> count = values.iterator();
			while(count.hasNext()){
				calcule=calcule+count.next().get();
			}
			System.out.println(calcule);
			context.write(key, new DoubleWritable(calcule));


		}
	}

	static class SecondReduce extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

		protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{

			Double calcule=(double) 0;
			Iterator<DoubleWritable> count = values.iterator();
			while(count.hasNext()){
				calcule=calcule+count.next().get();
			}
			System.out.println(calcule);
			context.write(key, new DoubleWritable(calcule));
		}
	}

	public static void job(Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {
		// First job
		Job job1 = Job.getInstance(conf);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setMapperClass(FirstMap.class);
		job1.setReducerClass(FirstReduce.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job1, new Path[]{new Path(conf.get("initialVectorPath")), new Path(conf.get("inputMatrixPath"))});
		FileOutputFormat.setOutputPath(job1, new Path(conf.get("intermediaryResultPath")));

		job1.waitForCompletion(true);

		Job job2 = Job.getInstance(conf);
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(DoubleWritable.class);

		job2.setMapperClass(SecondMap.class);
		job2.setReducerClass(SecondReduce.class);

		/* If your implementation of the combiner passed the unit test, uncomment the following line*/
		//job2.setCombinerClass(CombinerForSecondMap.class);

		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job2, new Path(conf.get("intermediaryResultPath")));
		FileOutputFormat.setOutputPath(job2, new Path(conf.get("currentVectorPath")));

		job2.waitForCompletion(true);

		FileUtils.deleteQuietly(new File(conf.get("intermediaryResultPath")));
	}

}

