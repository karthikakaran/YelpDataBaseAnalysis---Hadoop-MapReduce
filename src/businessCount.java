import java.io.IOException;
import java.util.HashSet;

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

public class businessCount extends Configured implements Tool {
	
	private static HashSet<String> busId = new HashSet<String>();
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private Text addresses = new Text();
		private IntWritable count = new IntWritable();
		
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			
			String[] businessDetails = value.toString().split("\\^");
			String outputText = "";
			String businessId =  businessDetails[0];
			if(!busId.contains(businessId)){
				busId.add(businessId);
				if(businessDetails[1].contains("Palo Alto")){
					outputText += businessDetails[1];		
					addresses.set(outputText);
					count.set(businessDetails[2].split(",").length);
				
					context.write(addresses, count);
				}
			}
		}
	}
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {

		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable i : values)
				sum += i.get();
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: businessCount <ip file> <out file>");
			System.exit(2);
		}

		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();

		job.setJobName("BusinessCount of Palo alto");
	//	DistributedCache.addCacheFile(new URI("hdfs://cshadoop1"+args[0]),conf);

		job.setJarByClass(businessCount.class);
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
		int exitCode = ToolRunner.run(new Configuration(), new businessCount(), args);
		System.exit(exitCode);
	}
}