package org.mahedi.bigdata.mapreduce.mapper;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private Map<Text, IntWritable> tfMap = new HashMap<Text, IntWritable>();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		tfMap.put(key, new IntWritable(sum));
		context.write(key, new IntWritable(sum));
	}

	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		Map<Text, IntWritable> sortedMap = sortByValue(tfMap);

		int counter = 0;
		for (Text key : sortedMap.keySet()) {
			//if (counter++ == 20) {
			//	break;
			//}
			context.write(key, sortedMap.get(key));
		}
	}

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
}
