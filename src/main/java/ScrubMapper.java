import java.io.IOException;

import opennlp.tools.tokenize.Tokenizer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import opennlp.tools.tokenize.SimpleTokenizer;


public class ScrubMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Tokenizer tokenizer = SimpleTokenizer.INSTANCE;
	
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String entry = value.toString();
		
		//grab the docID from before first comma
		String docID = entry.substring(0, entry.indexOf(","));

		//content begins after third comma
		int contentIdx = StringUtils.ordinalIndexOf(entry, ",", 3);
		String docContent = entry.substring(contentIdx + 1);

		// generate invert index
		generateInvertIndex(docID, docContent, context);
	}
	
	void generateInvertIndex(String id, String content, Context context) throws IOException, InterruptedException {
		// case folding
		content = content.toLowerCase();
		
		// tokenize content
		String[] tokens = tokenizer.tokenize(content);
		
		for (String token : tokens) {
			// remove punctuation
			if (token.matches("\\W+")) { continue; }
			// remove underbar
			if (token.matches("\\_+")) { continue; } 
			// remove non zipcode numbers
			if (token.matches("\\d+") && token.length() != 5) { continue; }
			// remove stop words
			// TODO
		
			// output the result of mapper
			context.write(new Text(token), new Text(id));	
		}
		
	}

}
