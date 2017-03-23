package org.mahedi.bigdata.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

/**
 * This class find the TF of all the words. Each word has the information of corresponding user
 * 
 * @author Md. Mahedi Kayasr(md.kaysar2@mail.dcu.ie, id:16213961)
 *
 */
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

		Job csvJob = Job.getInstance(getConf(), "Stack Exchange ETL: Count Words");
		csvJob.setJarByClass(StackExchangeTFIDF.class);

		csvJob.setMapperClass(TFMapper.class);

		csvJob.setReducerClass(TFReducer.class);
		csvJob.setNumReduceTasks(1);
		// key = word
		csvJob.setOutputKeyClass(Text.class);
		// value = TF-IDF
		csvJob.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(csvJob, new Path(args[0]));
		csvJob.setInputFormatClass(CSVTextInputFormat.class);

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
	public static class TFMapper extends Mapper<Object, List<Text>, Text, Text> {

		private final static Text one = new Text("1");
		private Text word = new Text();

		@Override
		public void map(Object key, List<Text> values, Context context) throws IOException, InterruptedException {

			if (values.size() > 9) {
				if (!values.get(0).toString().equalsIgnoreCase("Id") && !values.get(9).toString().trim().isEmpty()) {
					List<String> tokens = getTokensWithoutStopWords(
							values.get(8).toString().replaceAll("\\n", "").replaceAll("[^a-zA-Z0-9\\s]", " "));
					for (String token : tokens) {
						String newkey = values.get(0).toString().trim() + "," + token;
						word.set(newkey);
						context.write(word, one);
					}

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
	 * Write TF of all words in (key = userid, value=word:tf) format
	 * 
	 * @author mahedi
	 *
	 */
	public static class TFReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int tf = 0;
			for (Text a : values) {
				tf += Integer.parseInt(a.toString());
			}
			String[] keytuple = key.toString().split(",");
			context.write(new Text(keytuple[0] + ","), new Text(keytuple[1] + ":" + tf));
			// key = userid, value=word:tf
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

}
