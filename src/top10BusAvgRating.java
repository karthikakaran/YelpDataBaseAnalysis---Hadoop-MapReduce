import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ToolRunner;

public class top10BusAvgRating extends Configured implements Tool {
	
	public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable>{
		
		private Text businessId = new Text();
		private final static FloatWritable rating = new FloatWritable();
		
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			
			String[] reviewDetails = value.toString().split("\\^");
			businessId.set(reviewDetails[2]);
			rating.set(Float.parseFloat(reviewDetails[3]));
			context.write(businessId, rating);
		}
	}
	public static class Reduce extends Reducer<Text,FloatWritable,Text,FloatWritable> {

		HashMap<String, Float> sortRatings = new HashMap<String, Float>();
		public void reduce(Text key, Iterable<FloatWritable> values, Context context
				) throws IOException, InterruptedException {
			float sum = 0, avg = 0, ratingLen = 0;
			
			for(FloatWritable i : values){
				sum += i.get();
				ratingLen++;
			}
			avg = sum / ratingLen;
			sortRatings.put(key.toString(), avg);
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			HashMap<String, Float> sortedRatings = (HashMap<String, Float>) SortTopTenFloat.sortByValues(sortRatings);
			int i = 1;
			for (Entry<String, Float> result : sortedRatings.entrySet()) {
				if(i > 10) break;
				context.write(new Text(result.getKey()), new FloatWritable(result.getValue()));
				i++;
			}
		}
	}
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: top10BusAvgRating <ip file> <out file>");
			System.exit(2);
		}

		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();

		job.setJobName("Top 10 Businesses with Average Ratings");

		job.setJarByClass(top10BusAvgRating.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		FileSystem outFs = new Path(args[1]).getFileSystem(conf);
		outFs.delete(new Path(args[1]), true);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new top10BusAvgRating(), args);
		System.exit(exitCode);
	}
}