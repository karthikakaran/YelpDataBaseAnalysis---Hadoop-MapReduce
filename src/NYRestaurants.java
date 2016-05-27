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

public class NYRestaurants extends Configured implements Tool {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private Text businessIdKey = new Text();
		private Text addresses = new Text();
		
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			
			String[] businessDetails = value.toString().split("\\^");
				if(businessDetails[1].contains("NY") && businessDetails[2].contains("Restaurants")){		
					addresses.set(businessDetails[1]);
					businessIdKey.set(businessDetails[0]);
					context.write(businessIdKey, addresses);
				}
		}
	}
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context
				) throws IOException, InterruptedException {
			for(Text address : values)
			    result.set(address);
			context.write(key, result);
		}
	}
	
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: NYRestaurants <ip file> <out file>");
			System.exit(2);
		}

		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();

		job.setJobName("NY Restaurants");
		job.setJarByClass(NYRestaurants.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		FileSystem outFs = new Path(args[1]).getFileSystem(conf);
		outFs.delete(new Path(args[1]), true);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new NYRestaurants(), args);
		System.exit(exitCode);
	}
}