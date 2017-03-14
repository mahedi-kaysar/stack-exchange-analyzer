package org.mahedi.bigdata.hive.db.dao;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.mahedi.bigdata.hive.db.entity.Post;
import org.mahedi.bigdata.hive.db.manager.HiveDbManager;

/**
 * This DAO can be used for querying the Posts Tables
 * @author mahedi
 *
 */
public class PostDao {
	
	Connection connection = null;
	Statement statement = null;
	/**
	 * 
	 * @param N
	 *            is the number of expected rows
	 * @return list of top N post
	 */
	public List<Post> getTopNPostByScore(int N) {
		String query = "select id, title, body, viewcount, score, tags from posts order by score desc limit " + N;
		ResultSet rs = null;
		List<Post> posts = new ArrayList<>();
		try {
			connection = HiveDbManager.getInstance().getCon();
			statement = connection.createStatement();
			rs = statement.executeQuery(query);
			while (rs.next()) {
				String id = rs.getString(1);
				String title = rs.getString(2);
				String body = rs.getString(3);
				int viewCount = rs.getInt(4);
				int score = rs.getInt(5);
				String tags = rs.getString(6);
				Post post = new Post(id, title, body, viewCount, score, tags);
				posts.add(post);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (statement != null)
					statement.close();
				if (connection != null)
					connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		return posts;
	}

	/**
	 * 
	 * @param N
	 *            is the number of expected users
	 * @return (k,v) = (userId, totalViewCount)
	 */
	public Map<String, Integer> getTopNUserByTotalViewCount(int N) {
		String query = "select owneruserid, sum(viewcount) as viewc from posts GROUP BY owneruserid order by viewc desc limit "
				+ N;
		ResultSet rs = null;
		Map<String, Integer> userViewCountMap = new HashMap<>();
		try {
			connection = HiveDbManager.getInstance().getCon();
			statement = connection.createStatement();
			rs = statement.executeQuery(query);
			while (rs.next()) {
				String userId = rs.getString(1);
				int totalViewCount = rs.getInt(2);
				userViewCountMap.put(userId, totalViewCount);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (statement != null)
					statement.close();
				if (connection != null)
					connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		return userViewCountMap;
	}

	/**
	 * 
	 * @param word
	 *            is a word for searching in the post of every post
	 * @return list of Users/userId who used the word in his/her post
	 */
	public List<String> getDistinctUserUsedAWord(String word) {
		String query = "select distinct owneruserid from posts where instr(body, '" + word + "')!=0";
		ResultSet rs = null;
		List<String> users = new ArrayList<>();
		try {
			connection = HiveDbManager.getInstance().getCon();
			statement = connection.createStatement();
			rs = statement.executeQuery(query);
			while (rs.next()) {
				String userId = rs.getString(1);
				users.add(userId);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (statement != null)
					statement.close();
				if (connection != null)
					connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		return users;
	}

	public static void main(String[] args) {

		// Initialize the postDao
		PostDao postDao = new PostDao();

		System.out.println("getTopNPostByScore");
		List<Post> to10Posts = postDao.getTopNPostByScore(10);
		for (Post post : to10Posts)
			System.out.println(post);
		
		System.out.println("getTopNUserByTotalViewCount");
		Map<String, Integer> userViewCountMap = postDao.getTopNUserByTotalViewCount(10);
		for (Map.Entry<String, Integer> entry : userViewCountMap.entrySet()) {
			String key = entry.getKey();
			Integer value = entry.getValue();

			System.out.println("[UserID=" + key + ", TotalView=" + value + "]");
		}
		
		System.out.println("getDistinctUserUsedAWord");
		List<String> users = postDao.getDistinctUserUsedAWord("php");
		for (String userId : users)
			System.out.println(userId);
	}
}
