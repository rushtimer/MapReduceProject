
import java.io.IOException;
import java.io.PrintWriter;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapred.JobConf;
import java.util.*;
public class BigramCount {
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    
    private Text word = new Text();
    private Text bigram = new Text();
    private Map<String, Integer> map;
    // map method
    // How many times map method is called:
    // Answer: the number of lines in all files 
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String prev = null;	
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
      	String cur = itr.nextToken();	
      	if(prev != null) {
      		bigram.set(prev + " " + cur);
      		context.write(bigram,one);
      	}
      	prev = cur;
      }
      //context.write(new Text("@@__How many times map method is invoked"), one);
    }

    protected void cleanup(Context context)
      throws IOException, InterruptedException {
        Map<String, Integer> map = getMap();
        context.write(new Text("@@__number of map tasks: "), one);
    } //end of cleanup

	public Map<String, Integer> getMap() {
        if(null == map)
            map = new HashMap<String, Integer>();
        return map;
    } //end of getMap
}

  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "bigram");
    job.setJarByClass(BigramCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
/*
    PrintWriter writer = new PrintWriter("count_map.txt", "UTF-8");
	writer.println("Map method occurs " + count);
	writer.close();
*/