package org.mahedi.bigdata.mapreduce.mapper;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.mahedi.bigdata.mapreduce.common.Dictionary;

/**
 * This class maps (k,v) = (Id, post-columns) with proper cleaning and formating
 * 
 * @author mahedi
 *
 */
public class StackPostsHiveFormatMapper extends Mapper<LongWritable, List<Text>, Text, Text> {
	private static final Logger logger = Logger.getLogger(StackPostsHiveFormatMapper.class.getName());

	private Set<String> stopWords = Dictionary.stopWords();

	/**
	 * This map is responsible for cleaning the CSV columns and formating the
	 * data as hive expected file format.
	 */
	public void map(LongWritable key, List<Text> values, Context context) throws IOException, InterruptedException {
		logger.info("TestMapper");
		logger.info("key=" + key);

		StringBuffer sb = new StringBuffer();
		if (!values.get(0).toString().equalsIgnoreCase("Id")) {
			for (int i = 1; i < values.size(); i++) {
				if (i == 8 || i == 16) {
					String bodyTags = values.get(i).toString().replaceAll("\\n", "").replaceAll("[^a-zA-Z0-9\\s]", " ");
					// if (!bodyTags.equalsIgnoreCase("Body") &&
					// !bodyTags.equalsIgnoreCase("Tags"))
					sb.append(removeStopWords(bodyTags));
					// else
					// sb.append(values.get(i));
					sb.append(",");
				} else if (i != 3 && i != 5 && i != 10 && i != 12 && i != 20 && i != 21) {
					sb.append(values.get(i));
					if (i < 19)
						sb.append(",");
				}
			}
			Text k = new Text(values.get(0).toString()+",");
			context.write(k, new Text(sb.toString()));
		}
	}

	private String removeStopWords(String s) {
		String[] tokens = s.split("\\s+");
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < tokens.length; i++) {
			if (!tokens[i].isEmpty() && !stopWords.contains(tokens[i].toLowerCase())) {
				sb.append(tokens[i] + " ");
			}
		}
		return sb.toString();

	}
}