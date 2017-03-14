package org.mahedi.bigdata.mapreduce.reducer;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This mapper maps top N post by sorting the posts by score
 * 
 * @author mahedi
 *
 */
public class TopNPostByScoreReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

	@Override
	protected void reduce(Text arg0, Iterable<LongWritable> arg1,
			Reducer<Text, LongWritable, Text, LongWritable>.Context arg2) throws IOException, InterruptedException {
		// TODO need to implement
		super.reduce(arg0, arg1, arg2);
	}
	

	@Override
	protected void cleanup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO need to implement sorting
		super.cleanup(context);
	}
}
