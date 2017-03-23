package org.mahedi.bigdata.mapreduce.common;

import java.util.HashSet;
import java.util.Set;

/**
 * This class contains the different types of word's dictionary
 * For example, stop words
 * 
 * @author Md. Mahedi Kayasr(md.kaysar2@mail.dcu.ie, id:16213961)
 *
 */
public class Dictionary {
	public static Set<String> stopWords() {
		Set<String> stopWords = new HashSet<String>();
		stopWords.add("I");
		stopWords.add("a");
		stopWords.add("about");
		stopWords.add("an");
		stopWords.add("are");
		stopWords.add("as");
		stopWords.add("at");
		stopWords.add("be");
		stopWords.add("by");
		stopWords.add("com");
		stopWords.add("de");
		stopWords.add("en");
		stopWords.add("for");
		stopWords.add("from");
		stopWords.add("how");
		stopWords.add("in");
		stopWords.add("is");
		stopWords.add("it");
		stopWords.add("la");
		stopWords.add("of");
		stopWords.add("on");
		stopWords.add("or");
		stopWords.add("that");
		stopWords.add("the");
		stopWords.add("this");
		stopWords.add("to");
		stopWords.add("was");
		stopWords.add("what");
		stopWords.add("when");
		stopWords.add("where");
		stopWords.add("who");
		stopWords.add("will");
		stopWords.add("with");
		stopWords.add("and");
		stopWords.add("the");
		stopWords.add("www");
		stopWords.add("p");
		stopWords.add("pre");
		stopWords.add("code");
		return stopWords;
	}
}
