package org.mahedi.bigdata.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.mahedi.bigdata.mapreduce.common.Dictionary;
import org.mahedi.bigdata.mapreduce.common.MiscUtils;

public class StackExchangeTFIDF extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(StackExchangeTFIDF.class.getName());

	public StackExchangeTFIDF() {
		this(null);
	}

	public StackExchangeTFIDF(Configuration conf) {
		super(conf);
	}

	/**
	 * 
	 */
	public int run(String[] args) throws Exception {

		getConf().set(CSVLineRecordReader.FORMAT_DELIMITER, "\"");
		getConf().set(CSVLineRecordReader.FORMAT_SEPARATOR, ",");
		getConf().setInt(CSVNLineInputFormat.LINES_PER_MAP, 40000);
		getConf().setBoolean(CSVLineRecordReader.IS_ZIPFILE, false);
		getConf().set(CSVLineRecordReader.CSV_SEPARATOR, ",");

		Job csvJob = Job.getInstance(getConf(), "Stack Exchange ETL with TD_IDF");
		csvJob.setJarByClass(StackExchangeTFIDF.class);
		// csvJob.setNumReduceTasks(1);

		// csvJob.setMapperClass(WordFrequencyMapper.class);
		csvJob.setMapperClass(TopNMapper.class);

		// csvJob.setCombinerClass(TopNPostByScoreCombiner.class);
		// csvJob.setReducerClass(WordFrequencyReducer.class);
		csvJob.setReducerClass(TopNReducer.class);

		csvJob.setInputFormatClass(CSVTextInputFormat.class);
		csvJob.setOutputKeyClass(Text.class);
		csvJob.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(csvJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(csvJob, new Path(args[1]));

		logger.info("Process will begin");

		csvJob.waitForCompletion(true);

		logger.info("Process ended");

		return 0;
	}

	public static void main(String[] args) throws Exception {

		int res = -1;
		try {
			logger.info("Initializing StackExchangeTFIDF for CSV reader");
			StackExchangeTFIDF importer = new StackExchangeTFIDF();
			// Let ToolRunner handle generic command-line options and run hadoop
			res = ToolRunner.run(new Configuration(), importer, args);
			logger.info("ToolRunner finished running hadoop");

		} catch (Throwable e) {
			throw new Exception(e);
		} finally {
			logger.info("Quitting with execution code " + res);
			System.exit(res);
		}
	}

	/**
	 * The mapper reads one line at the time, splits it into an array of single
	 * words and emits every word to the reducers with the value of 1.
	 */
	public static class TopNMapper extends Mapper<Object, List<Text>, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(Object key, List<Text> values, Context context) throws IOException, InterruptedException {

			if (!values.get(0).toString().equalsIgnoreCase("Id")) {
				List<String> tokens = getTokensWithoutStopWords(
						values.get(8).toString().replaceAll("\\n", "").replaceAll("[^a-zA-Z0-9\\s]", " "));
				for (String token : tokens) {
					word.set(token);
					context.write(word, one);
				}

			}
		}

		private List<String> getTokensWithoutStopWords(String s) {
			String[] tokens = s.trim().split("\\s+");
			List<String> tokenList = new ArrayList<>();
			for (int i = 0; i < tokens.length; i++) {
				if (!tokens[i].isEmpty() && !Dictionary.stopWords().contains(tokens[i].toLowerCase())
						&& Character.isLetter(tokens[i].charAt(0)) && !Character.isDigit(tokens[i].charAt(0))
						&& !tokens[i].contains("_") && tokens[i].length() >= 3) {
					tokenList.add(tokens[i]);
				}
			}
			return tokenList;

		}
	}

	/**
	 * The reducer retrieves every word and puts it into a Map: if the word
	 * already exists in the map, increments its value, otherwise sets it to 1.
	 */
	public static class TopNReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private Map<Text, IntWritable> countMap = new HashMap<>();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			countMap.put(new Text(key), new IntWritable(sum));
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			Map<Text, IntWritable> sortedMap = MiscUtils.sortByValues(countMap);

			int counter = 0;
			for (Text key : sortedMap.keySet()) {
				if (counter++ == 10) {
					break;
				}
				context.write(new Text(key.toString()+","), sortedMap.get(key));
			}
		}
	}

	/**
	 * The combiner retrieves every word and puts it into a Map: if the word
	 * already exists in the map, increments its value, otherwise sets it to 1.
	 */
	public static class TopNCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

}
