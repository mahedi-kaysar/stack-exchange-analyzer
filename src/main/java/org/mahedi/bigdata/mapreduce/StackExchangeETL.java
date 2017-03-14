package org.mahedi.bigdata.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.mahedi.bigdata.mapreduce.mapper.StackPostsHiveFormatMapper;

/**
 * This class do ETL job for StackExchange Dataset (CSV) which has been
 * downloaded from StackExchange Explorer by the query: <i>select top 200 * from
 * posts where posts.ViewCount > 1000000 ORDER BY posts.ViewCount</i>
 * 
 * Finally, it saves the result as a hive formatted file.
 * 
 * @author mahedi
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

}
