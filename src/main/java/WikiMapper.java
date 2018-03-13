//Networked Information Systems
//Lab 2: Inverted Index
//Thomas Willkens
//February 28, 2018

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.englishStemmer;

public class WikiMapper extends Mapper<LongWritable, Text, Text, Text> {

	//get standard set of stopwords from Stanford CoreNLP's StopwordAnnotator,
	//available on Maven central repo: 
	//https://github.com/plandes/stopword-annotator 
	//https://mvnrepository.com/artifact/com.zensols/stopword-annotator
	private Set<?> stopWords = StopAnalyzer.ENGLISH_STOP_WORDS_SET;

	//SnowballStemmer: available on Maven central repo:
	//https://github.com/rholder/snowball-stemmer
	//https://mvnrepository.com/artifact/com.github.rholder/snowball-stemmer/1.3.0.581.1 
	SnowballStemmer stemmer = new englishStemmer();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, FileNotFoundException {

		String entry = value.toString();
		
		//grab the docnum from before first comma
		String docNum = entry.substring(0, entry.indexOf(","));

		//content begins after third comma
		int contentIdx = StringUtils.ordinalIndexOf(entry, ",", 3);

		//grab content after third comma and split by spaces, punctuation, slashes, 
		//dashes, etc.
		String[] contentArr = entry.substring(contentIdx + 1).split("[ \\-—\\/.,;:]");

		//add content to ArrayList for easy iterating
		List<String> content = new ArrayList<String>(Arrays.asList(contentArr));

		for (String word : content) {
			//process word with helper method
			word = processWord(word);
			
			//if processed word passes validation checks, pass to reducer
			//along with docNum after converting to Text
			if (!word.equals("")) {
				context.write(new Text(word), new Text(docNum));
			}
		}
	}

	private String processWord(String word) {
		//use a regex to retain only unicode latin characters
		word = word.toLowerCase().replaceAll("[^a-zA-Z\u00C0-\u00FF]", "");

		//if word is eliminated or a stopword, return empty string
		if (word.equals("") || stopWords.contains(word)) {
			return "";
		}

		//stem the word
		//usage tips from: https://github.com/abh1nav/libstemmer
		stemmer.setCurrent(word);
		stemmer.stem();
		word = stemmer.getCurrent();
		return word;
	}
}
