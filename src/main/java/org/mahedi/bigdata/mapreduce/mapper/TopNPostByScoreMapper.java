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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This map task is responsible for mapping (K, V) = (PostID, Score) And sort
 * the result by score in descending order Also write top N posts by its score
 * 
 * @author mahedi
 *
 */
public class TopNPostByScoreMapper extends Mapper<LongWritable, List<Text>, Text, LongWritable> {

	private Map<Text, LongWritable> scoreMap = new HashMap<Text, LongWritable>();

	/**
	 * 
	 */
	@Override
	protected void map(LongWritable key, List<Text> values,
			Mapper<LongWritable, List<Text>, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		String rowStr = values.get(0).toString();
		String[] row = rowStr.split(",");

		// include only the score field
		if (!row[0].equals("Id")) {

			Text k = new Text(row[0]);
			LongWritable v = new LongWritable(Long.parseLong(row[6]));
			scoreMap.put(k, v);
			// context.write(k, v);
		}
	}

	/**
	 * 
	 */
	@Override
	protected void cleanup(Mapper<LongWritable, List<Text>, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		Map<Text, LongWritable> sortedMap = sortByValue(scoreMap);

		int counter = 0;
		for (Text key : sortedMap.keySet()) {
			if (counter++ == 10) {
				break;
			}
			context.write(key, sortedMap.get(key));
		}
	}

	private Map<Text, LongWritable> sortByValue(Map<Text, LongWritable> scoreMap) {

		List<Map.Entry<Text, LongWritable>> list = new LinkedList<Map.Entry<Text, LongWritable>>(scoreMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<Text, LongWritable>>() {
			public int compare(Map.Entry<Text, LongWritable> o1, Map.Entry<Text, LongWritable> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});

		Map<Text, LongWritable> result = new LinkedHashMap<Text, LongWritable>();
		for (Map.Entry<Text, LongWritable> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}
}
