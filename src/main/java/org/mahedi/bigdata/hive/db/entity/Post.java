package org.mahedi.bigdata.hive.db.entity;

/**
 * It represents a Post
 * 
 * @author mahedi
 *
 */
public class Post {
	private String id;
	private String title;
	private String body;
	private int viewCount;
	private int score;
	private String tags;

	public Post() {
		super();
	}

	public Post(String id, String title, String body, int viewCount, int score, String tags) {
		super();
		this.id = id;
		this.title = title;
		this.body = body;
		this.viewCount = viewCount;
		this.score = score;
		this.tags = tags;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	public int getViewCount() {
		return viewCount;
	}

	public void setViewCount(int viewCount) {
		this.viewCount = viewCount;
	}

	public int getScore() {
		return score;
	}

	public void setScore(int score) {
		this.score = score;
	}

	public String getTags() {
		return tags;
	}

	public void setTags(String tags) {
		this.tags = tags;
	}

	@Override
	public String toString() {
		return "Post [id=" + id + ", title=" + title + ", viewCount=" + viewCount + ", score=" + score + ", tags="
				+ tags + "]";
	}

}
