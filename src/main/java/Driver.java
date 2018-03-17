//Networked Information Systems
//Lab 2: Inverted Index
//Thomas Willkens
//February 28, 2018

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


//Usage: add the three csv files to the root project directory and run.
//Run configuration arguments: <wiki_00.csv> <wiki_01.csv> <wiki_02.csv> <output_dir>
public class Driver {
	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException {
		JobControl jobControl = new JobControl("jobChain"); 
	    
		Configuration conf1 = new Configuration();
	    Job wcjob = Job.getInstance(conf1);  
	    wcjob.setJarByClass(Driver.class);
	    wcjob.setMapperClass(TokenMapper.class);
	    wcjob.setCombinerClass(SumReducer.class);
	    wcjob.setReducerClass(SumReducer.class);
	    wcjob.setJobName("Word Count");
	    TextInputFormat.addInputPath(wcjob, new Path("/Users/ronnyking01/Documents/132B/lab2/lab2_data"));
	    wcjob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(wcjob, new Path("/Users/ronnyking01/Documents/132B/output/wc"));
		wcjob.setOutputKeyClass(Text.class);
		wcjob.setOutputValueClass(IntWritable.class);
		wcjob.waitForCompletion(true);
	    ControlledJob controlledJob1 = new ControlledJob(conf1);
	    controlledJob1.setJob(wcjob);
	    jobControl.addJob(controlledJob1);
	    
		Configuration conf2 = new Configuration();
		conf2.set("threshold", "1000");
	    Job swjob = Job.getInstance(conf2);  
	    swjob.setJarByClass(Driver.class);
	    swjob.setMapperClass(ThresholdMapper.class);
	    swjob.setReducerClass(SimpleReducer.class);
	    swjob.setJobName("Wordcount Process");
	    TextInputFormat.addInputPath(swjob, new Path("/Users/ronnyking01/Documents/132B/output/wc"));
	    swjob.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(wcjob, new Path("/Users/ronnyking01/Documents/132B/output/temp"));
		swjob.setOutputKeyClass(Text.class);
		swjob.setOutputValueClass(Text.class);
		swjob.waitForCompletion(true);
	    ControlledJob controlledJob2 = new ControlledJob(conf2);
	    controlledJob2.setJob(swjob);
	    jobControl.addJob(controlledJob2);
	    
	   // Set<String> stopwords = processoutput(args[1] + "/temp");
	    
//	    Configuration conf3 = new Configuration();
//
//	    Job indexjob = Job.getInstance(conf2);  
//	    indexjob.setJarByClass(Driver.class);
//		indexjob.setMapperClass(WikiMapper.class);
//		indexjob.setOutputKeyClass(Text.class);
//		indexjob.setOutputValueClass(Text.class);
//		indexjob.setReducerClass(WikiReducer.class);
//		indexjob.waitForCompletion(true);
//
//	    TextInputFormat.addInputPath(indexjob, new Path(args[0]));
//	    indexjob.setOutputFormatClass(TextOutputFormat.class);
//		TextOutputFormat.setOutputPath(indexjob, new Path(args[1] + "/out"));
//
//		
//		
//	    ControlledJob controlledJob3 = new ControlledJob(conf3);
//	    controlledJob3.setJob(wcjob);
//
//	    jobControl.addJob(controlledJob3);
//	    
	 

	    controlledJob2.addDependingJob(controlledJob1); 
	//    controlledJob3.addDependingJob(controlledJob2); 

	    Thread jobControlThread = new Thread(jobControl);
	    jobControlThread.start();


	}

	private static Set<String> processoutput(String path) {
		
		return null;
	}
}