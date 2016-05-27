import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class top10BusZip extends Configured implements Tool {
	
	private static HashSet<String> busId = new HashSet<String>();
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private Text zipCodeRes = new Text();
		private final static IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			
			String[] businessDetails = value.toString().split("\\^");
			String zipCode = "";
			String businessId =  businessDetails[0];
			if(!busId.contains(businessId)){
				busId.add(businessId);
				zipCode = businessDetails[1].substring(businessDetails[1].lastIndexOf(" "));
				zipCodeRes.set(zipCode);
				context.write(zipCodeRes, one);
			}
		}
	}
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {

		HashMap<String, Integer> sortZip = new HashMap<String, Integer>();
		public void reduce(Text key, Iterable<IntWritable> values, Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable i : values)
				sum += i.get();
			sortZip.put(key.toString(), sum);
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> sortedZip = (HashMap<String, Integer>) SortTopTen.sortByValues(sortZip);
			int i = 1;
			for (Entry<String, Integer> result : sortedZip.entrySet()) {
				if(i > 10) break;
				context.write(new Text(result.getKey()), new IntWritable(result.getValue()));
				i++;
			}
		}
	}
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: top10BusZip <ip file> <out file>");
			System.exit(2);
		}

		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();

		job.setJobName("Top 10 Zipcodes with business ");

		job.setJarByClass(top10BusZip.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		FileSystem outFs = new Path(args[1]).getFileSystem(conf);
		outFs.delete(new Path(args[1]), true);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new top10BusZip(), args);
		System.exit(exitCode);
	}
}