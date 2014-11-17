package edu.ncut.decloud.hadoopinaction;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Count {

  public static class MyMapper 
       extends Mapper<Text, Text, Text, Text>{
          
    public void map(Text key, Text value, Context context
                    ) throws IOException, InterruptedException {
      context.write(value, key);
    }
  }
  
  public static class MyReducer 
       extends Reducer<Text,Text,Text,IntWritable> {
    //private IntWritable result = new IntWritable();
 
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int count = 0;
      for (Text text : values) {
		count++;
      }
      
      context.write(key, new IntWritable(count));
      
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    System.out.println(args[0]);
    // set k-v separator
    
    conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
    @SuppressWarnings("deprecation")
	Job job = new Job(conf, "CcccccC");
    job.setJarByClass(Count.class);
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    job.setMapperClass(MyMapper.class);
    //comment this combiner the program will be ok.
    //加上combiner须更改对应的输出 k/v格式 
//    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);
    
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
   
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
