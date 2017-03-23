package org.mahedi.bigdata;

import org.mahedi.bigdata.hive.db.dao.PostDao;
import org.mahedi.bigdata.mapreduce.StackExchangeETL;
import org.mahedi.bigdata.mapreduce.StackExchangeTFIDF;
import org.mahedi.bigdata.mapreduce.StackExchangeTopN_TFIDF;
/**
 * Main Launcher for all other main class to run.
 * Pass the desired class name as a first argument
 * 
 * @author Md. Mahedi Kayasr(md.kaysar2@mail.dcu.ie, id:16213961) 
 *
 */
public class AssignmentOneLauncher {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args == null || args.length < 1)
			System.exit(0);
		// args[0] must be full class name with package.
		String mainClassToRun = args[0];
		if (mainClassToRun.equals(PostDao.class.getName())) {
			try {
				PostDao.main(args);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {

			if (args.length != 3)
				System.exit(0);

			// args[1] and args[2] is the hdfs input and output location
			String[] newArgs = new String[args.length - 1];
			System.arraycopy(args, 1, newArgs, 0, newArgs.length);

			if (mainClassToRun.equals(StackExchangeETL.class.getName())) {
				try {
					StackExchangeETL.main(newArgs);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else if (mainClassToRun.equals(StackExchangeTFIDF.class.getName())) {
				try {
					StackExchangeTFIDF.main(newArgs);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else if (mainClassToRun.equals(StackExchangeTopN_TFIDF.class.getName())) {
				try {
					StackExchangeTopN_TFIDF.main(newArgs);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

}
