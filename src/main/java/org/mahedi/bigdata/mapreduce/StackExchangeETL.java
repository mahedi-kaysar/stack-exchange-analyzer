package org.mahedi.bigdata.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.mahedi.bigdata.mapreduce.common.Dictionary;

/**
 * This class do ETL job for StackExchange Dataset (CSV) which has been
 * downloaded from StackExchange Explorer
 * 
 * Finally, it saves the result as a hive formatted file.
 * 
 * @author Md. Mahedi Kayasr(md.kaysar2@mail.dcu.ie, id:16213961)
 *
 */
public class StackExchangeETL extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(StackExchangeETL.class.getName());

	public StackExchangeETL() {
		this(null);
	}

	public StackExchangeETL(Configuration conf) {
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
		
		Job csvJob = Job.getInstance(getConf(), "Stack Exchange ETL");
		csvJob.setJarByClass(StackExchangeETL.class);
		csvJob.setNumReduceTasks(0);
		
		csvJob.setMapperClass(StackPostsHiveFormatMapper.class);

		csvJob.setInputFormatClass(CSVTextInputFormat.class);
		csvJob.setOutputKeyClass(Text.class);
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
			logger.info("Initializing StackExchangeETL for CSV reader");
			StackExchangeETL importer = new StackExchangeETL();
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
	 * This class maps (k,v) = (Id, post-columns) with proper cleaning and formating
	 * 
	 * @author mahedi
	 *
	 */
	public static class StackPostsHiveFormatMapper extends Mapper<LongWritable, List<Text>, Text, Text> {
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
			if(values.size()>9){
				if (!values.get(0).toString().equalsIgnoreCase("Id") && !values.get(9).toString().trim().isEmpty()) {
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
}