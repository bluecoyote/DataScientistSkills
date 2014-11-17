package edu.ncut.decloud.hadoopinaction;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Myjob {

  public static class MyMapper 
       extends Mapper<Text, Text, Text, Text>{
          
    public void map(Text key, Text value, Context context
                    ) throws IOException, InterruptedException {
      context.write(value, key);
    }
  }
  
  public static class MyReducer 
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      String csv = "";
      for (Text text : values) {
    	  if (csv.length() > 0) {
				csv += ",";
			}
    	  csv += text.toString();
      }
      context.write(key, new Text(csv));
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
	  Job job = new Job(conf, "CcC");
    job.setJarByClass(Myjob.class);
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);
    
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
   
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}