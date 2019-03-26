import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;

public class invertedindex {

	public static class InvertedIndexMapper extends Mapper<LongWritable,Text,wordpair,Text> {
		// Declare associative array as globle variable
		private HashMap<String, Integer> hashmap = null;
		
		public HashMap<String, Integer> getMap() {
			if(hashmap == null) 
				hashmap = new HashMap<String, Integer>();
			return hashmap;
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
		  throws IOException,InterruptedException {
			hashmap = getMap();
			/*Get the name of the file using context.getInputSplit()method*/
		    //String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		    String line=value.toString();
		    //Split the line in words
		    String words[]=line.split(" ");
		    // Go through each word, store each word as key, 
		    // count how many times that word appears as value, set them to hashmap 
		    for(String s: words) {
		    	if(hashmap.containsKey(s)) {
		    		int count = hashmap.get(s) + 1;
		    		hashmap.put(s,count);
		    	} else {
		    		hashmap.put(s, 1);
		    	}	
		    }
		}

		protected void cleanup(Context context)
		  throws IOException, InterruptedException {
			/*Get the name of the file using context.getInputSplit()method*/
	    	String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	    	hashmap = getMap();
	    	Iterator<Map.Entry<String, Integer>> it = hashmap.entrySet().iterator();
	    	while(it.hasNext()) {
	    		Map.Entry<String, Integer> entry = it.next();
	    		String word = entry.getKey();
	    		int count = entry.getValue();
	    		Text value = new Text(fileName + ":" + Integer.toString(count));
	    		context.write(new wordpair(word,fileName), value);
	    	}
		}
	}

	public static class InvertedIndexReducer extends Reducer<wordpair, Text, Text, Text> {
		private Text currentWord = new Text();
	    private Text empty = new Text();
	    private String p = "";

	    @Override
	    public void reduce(wordpair key, Iterable<Text> values, Context context)
	      throws IOException, InterruptedException {
	      	// reducer receives intermediate pairs from mapper
	      	// (word, fileName),[#]
	        if (key.getWord().equals(currentWord)) { 
	            for(Text t : values) {
	            	p = p + t.toString() + ";";
	            }     
	        } else {
	            if(!currentWord.equals(empty)){
	            	p = p.substring(0,p.length() -1);
	                context.write(currentWord, new Text(p));
	                p = "";
	            }
	            currentWord.set(key.getWord());
	            for(Text t : values){
	                p = p + t.toString() + ";";
	            }
	        }  
    	}

        protected void cleanup(Context context)throws IOException, InterruptedException {
            p = p.substring(0,p.length() -1);
          context.write(currentWord, new Text(p));
        }
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: inverted_index");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "inverted_index");
	    job.setJarByClass(invertedindex.class);
	    job.setJobName("inverted_index");

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    job.setMapperClass(InvertedIndexMapper.class);
	    job.setReducerClass(InvertedIndexReducer.class);
	    job.setNumReduceTasks(3);

	    job.setOutputKeyClass(wordpair.class);
	    job.setOutputValueClass(Text.class);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


}


