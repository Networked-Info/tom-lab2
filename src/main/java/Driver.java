//Networked Information Systems
//Lab 2: Inverted Index
//Thomas Willkens
//February 28, 2018

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//Usage: add the three csv files to the root project directory and run.
//Run configuration arguments: <wiki_00.csv> <wiki_01.csv> <wiki_02.csv> <output_dir>
public class Driver {
	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException {
		Path file1 = new Path(args[0]);
		Path file2 = new Path(args[1]);
		Path file3 = new Path(args[2]);
		Path out = new Path(args[3]);
		Configuration conf = new Configuration(true);
		Job job = Job.getInstance(conf, "inverted index");
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, file1);
		TextInputFormat.addInputPath(job, file2);
		TextInputFormat.addInputPath(job, file3);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, out);
		job.setJarByClass(Driver.class);
		job.setMapperClass(WikiMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(WikiReducer.class);
		job.waitForCompletion(true);

	}
}