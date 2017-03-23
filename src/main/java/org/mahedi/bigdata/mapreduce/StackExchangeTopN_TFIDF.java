package org.mahedi.bigdata.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.mahedi.bigdata.mapreduce.common.MiscUtils;

/**
 * This class finds the top N words per user.
 * It takes input from the output of StackExchangeTFIDF class
 * 
 * @author Md. Mahedi Kayasr(md.kaysar2@mail.dcu.ie, id:16213961)
 *
 */
public class StackExchangeTopN_TFIDF extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(StackExchangeTopN_TFIDF.class.getName());

	public StackExchangeTopN_TFIDF() {
		this(null);
	}

	public StackExchangeTopN_TFIDF(Configuration conf) {
		super(conf);
	}

	public int run(String[] args) throws Exception {

		getConf().set(CSVLineRecordReader.FORMAT_DELIMITER, "\"");
		getConf().set(CSVLineRecordReader.FORMAT_SEPARATOR, ",");
		getConf().setInt(CSVNLineInputFormat.LINES_PER_MAP, 40000);
		getConf().setBoolean(CSVLineRecordReader.IS_ZIPFILE, false);
		getConf().set(CSVLineRecordReader.CSV_SEPARATOR, ",");
		Job csvJob = Job.getInstance(getConf(), "Stack Exchange ETL with Top_N_TFIDF_PerUser");
		csvJob.setJarByClass(StackExchangeTopN_TFIDF.class);
		csvJob.setMapperClass(TFByUserMapper.class);
		csvJob.setReducerClass(TFByUserReducer.class);
		csvJob.setInputFormatClass(TextInputFormat.class);
		// key = word
		csvJob.setOutputKeyClass(Text.class);
		// value = TF-IDF
		csvJob.setOutputValueClass(Text.class);
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
			logger.info("Initializing StackExchangeTopN_TFIDF for CSV reader");
			StackExchangeTopN_TFIDF importer = new StackExchangeTopN_TFIDF();
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
	 * Read TF for all users
	 * 
	 * @author mahedi
	 *
	 */
	public static class TFByUserMapper extends Mapper<LongWritable, Text, Text, Text> {

		// value=userid,word:tf
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			Text userId = new Text(tokens[0]);
			Text wordTFpair = new Text(tokens[1].trim());
			context.write(userId, wordTFpair);
		}
	}

	/**
	 * Top 10 words per user
	 * 
	 * @author mahedi
	 *
	 */
	public static class TFByUserReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Map<Text, IntWritable> wordTFMap = new HashMap<>();
			for (Text tuple : values) {
				String[] pairs = tuple.toString().split(":");
				wordTFMap.put(new Text(pairs[0]), new IntWritable(Integer.parseInt(pairs[1])));
			}
			Map<Text, IntWritable> sortedMap = MiscUtils.sortByValues(wordTFMap);
			// top 10 words for each user
			int counter = 0;
			for (Map.Entry<Text, IntWritable> entry : sortedMap.entrySet()) {
				if (counter++ == 10)
					break;
				context.write(new Text(key.toString() + ","), new Text(entry.getKey() + ":" + entry.getValue()));
			}
			wordTFMap = null;
		}
	}

}
