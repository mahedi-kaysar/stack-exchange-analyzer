package org.mahedi.bigdata.mapreduce.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.mahedi.bigdata.mapreduce.common.Dictionary;

public class WordFrequencyMapper extends Mapper<LongWritable, List<Text>, Text, IntWritable> {
	private Map<Text, IntWritable> tfMap = new HashMap<Text, IntWritable>();

	@Override
	protected void map(LongWritable key, List<Text> values,
			Mapper<LongWritable, List<Text>, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		//Pattern p = Pattern.compile("\\w+");
		//Matcher m = p.matcher(values.get(8).toString());

		if(!values.get(0).toString().equalsIgnoreCase("Id")){
			List<String> tokens = getTokensWithoutStopWords(values.get(8).toString().replaceAll("\\n", "").replaceAll("[^a-zA-Z0-9\\s]", " "));
			for(String token:tokens){	
				context.write(new Text(token), new IntWritable(1));
				//tfMap.put(new Text(token), new IntWritable(1));
			}
			
		}
//		while (m.find()) {
//			String matchedKey = m.group().toLowerCase();
//			// remove names starting with non letters, digits, considered
//			// stopwords or containing other chars and shorter length
//			if (!Character.isLetter(matchedKey.charAt(0)) || Character.isDigit(matchedKey.charAt(0))
//					|| Dictionary.stopWords().contains(matchedKey) || matchedKey.contains("_")
//					|| matchedKey.length()<3) {
//				continue;
//			}
//			valueBuilder.append(matchedKey);
//			context.write(new Text(valueBuilder.toString()), new IntWritable(1));
//		}
	}
//	@Override
//	protected void cleanup(Mapper<LongWritable, List<Text>, Text, IntWritable>.Context context)
//			throws IOException, InterruptedException {
//		Map<Text, IntWritable> sortedMap = sortByValue(tfMap);
//
//		int counter = 0;
//		for (Text key : sortedMap.keySet()) {
//			if (counter++ == 10) {
//				break;
//			}
//			context.write(key, sortedMap.get(key));
//		}
//	}
	private Map<Text, IntWritable> sortByValue(Map<Text, IntWritable> scoreMap) {

		List<Map.Entry<Text, IntWritable>> list = new LinkedList<Map.Entry<Text, IntWritable>>(scoreMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<Text, IntWritable>>() {
			public int compare(Map.Entry<Text, IntWritable> o1, Map.Entry<Text, IntWritable> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});

		Map<Text, IntWritable> result = new LinkedHashMap<Text, IntWritable>();
		for (Map.Entry<Text, IntWritable> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}

	private List<String> getTokensWithoutStopWords(String s) {
		String[] tokens = s.trim().split("\\s+");
		List<String> tokenList = new ArrayList<>();
		for (int i = 0; i < tokens.length; i++) {
			if (!tokens[i].isEmpty() && !Dictionary.stopWords().contains(tokens[i].toLowerCase()) 
					&& Character.isLetter(tokens[i].charAt(0)) && !Character.isDigit(tokens[i].charAt(0))
							&& !tokens[i].contains("_") && tokens[i].length()>=3) {
				tokenList.add(tokens[i]);
			}
		}
		return tokenList;

	}
}
