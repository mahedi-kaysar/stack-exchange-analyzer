package org.mahedi.bigdata.hive.db.manager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

/**
 * This class establishes connection to hive server through jdbc driver.
 * 
 * @author mahedi
 *
 */
public class HiveDbManager {

	private static final Logger logger = Logger.getLogger(HiveDbManager.class.getName());

	private static HiveDbManager hiveDbManager = null;

	private String DbUsername = "root";
	private String DbPassword = "";
	private String DbName = "default";
	private String driverClassName = "org.apache.hive.jdbc.HiveDriver";
	private String urlForManipulatingDb = "jdbc:hive2://10.211.55.101:10000/" + DbName;

	private HiveDbManager() {
		try {
			Class.forName(driverClassName);
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}
	}

	public Connection getCon() throws SQLException {
		Connection conn = null;
		conn = DriverManager.getConnection(urlForManipulatingDb, DbUsername, DbPassword);
		return conn;
	}

	public static HiveDbManager getInstance() {
		if (hiveDbManager == null) {
			hiveDbManager = new HiveDbManager();
		}
		return hiveDbManager;
	}
}
