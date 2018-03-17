//Networked Information Systems
//Lab 2: Inverted Index
//Thomas Willkens
//February 28, 2018

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimpleReducer extends Reducer<Text, Text, Text, Text> {
	
		
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context ctx)
								throws IOException, InterruptedException {


		//write to output
		ctx.write(key, new Text(" "));
	}
}