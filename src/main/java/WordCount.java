import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

	

	

	public static void wcdriver(String f1, String f2, String f3, String o) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "inverted index");
		Path file1 = new Path(f1);
		Path file2 = new Path(f2);
		Path file3 = new Path(f3);
		Path out = new Path(o);
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, file1);
		TextInputFormat.addInputPath(job, file2);
		TextInputFormat.addInputPath(job, file3);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, out);
		job.setJarByClass(WordCount.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
	}
}