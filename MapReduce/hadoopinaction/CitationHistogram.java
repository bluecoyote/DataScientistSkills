package edu.ncut.decloud.hadoopinaction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * Page61 - Page62, Hadoop 实战
 */
public class CitationHistogram {

	public static class MyMapper extends
			Mapper<Text, Text, IntWritable, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private final static IntWritable citationCount = new IntWritable();

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			citationCount.set(Integer.parseInt(value.toString()));
			context.write(citationCount, one);
		}
	}

	public static class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		// private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable text : values) {
				count++;
			}
			context.write(key, new IntWritable(count));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CitationHistogram");
		job.setJarByClass(edu.ncut.decloud.hadoopinaction.CitationHistogram.class);
		// TODO: specify a mapper
		job.setMapperClass(MyMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(MyReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(KeyValueTextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(IntWritable.class);
	    
		
		if (!job.waitForCompletion(true))
			return;
	}

}
