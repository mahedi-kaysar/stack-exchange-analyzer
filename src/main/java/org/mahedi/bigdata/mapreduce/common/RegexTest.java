package org.mahedi.bigdata.mapreduce.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * this class is not organized yet
 * need to make some utilities
 * 
 * @author mahedi
 *
 */
public class RegexTest {
	public static final String EXAMPLE_TEST = "This is my small example " + "string which I'm going to "
			+ "use for pattern matching.";
	public static final String body = "1244,1000729,<p>Executing the command "
			+ "<code>git clone git@github.com:whatever</code> "
			+ "creates a directory in my current folder named whatever, "
			+ "and drops the contents of the Git repository into that folder:</p>\n\n"
			+ "<pre><code>/httpdocs/whatever/public\n</code></pre>\n"
			+ "<p>My problem is that I need the contents of the Git repository cloned into "
			+ "my current directory so that they appear in the proper location for the web server:</p>\n\n"
			+ "<pre><code>/httpdocs/public\n</code></pre>\n"
			+ "<p>I know how to move the files after I've cloned the repository, "
			+ "but this seems to break Git, and I'd like to be able to update just by calling "
			+ "<code>git pull</code>. How can I do this?</p>,78642,BigDave,<p>hello, how are you?</p>,200,100";

	public static void main(String[] args) {
		// TODO Auto-generated method stub

//		System.out.println(EXAMPLE_TEST.matches("\\w.*"));
//		String[] splitString = (EXAMPLE_TEST.split("\\s+"));
//		System.out.println(splitString.length);// should be 14
//		for (String string : splitString) {
//			System.out.println(string);
//		}
//		// replace all whitespace with tabs
//		System.out.println(EXAMPLE_TEST.replaceAll("\\s+", "\t"));
		//System.out.println(body);
		//System.out.println(bodyArray[7]);
		
		// replace all new lines to empty character for making one line string
		String bodies = body.replaceAll("\\n", "");
		System.out.println(bodies);
		
		String regex = "p>";
		String[] bodyArray = bodies.split(regex);
		System.out.println(bodyArray.length);
		for(String s:bodyArray){
			//System.out.println(s);
		}
		String l="";
		for(String s:bodyArray){
			if(s.charAt(s.length()-1)=='<')
				l = l+s+"p>";
			else if(s.charAt(s.length()-1)=='/')
				l=l+"(.+)</p>";
			else
				l=l+s;
		}
		
		//System.out.println(l);
		
		String regex3 = bodyArray[0]+"p>(.+)</p>"+bodyArray[6];
		Pattern p1 = Pattern.compile(l);
		Matcher m1 = p1.matcher(bodies);
		while(m1.find()){
			System.out.println(m1.group(1).replaceAll(",", " "));
		}
		//Java regular expression to remove all non alphanumeric characters EXCEPT spaces

		//String pattern2 = "[^a-zA-Z0-9\\s]";
		//System.out.println(bodies.replaceAll(pattern2, " "));
		// split by ',' except searching in some certain regions like <p> </p> 
		
		//// Extract the text between the two p elements
		//String pattern1 = "(?i)(<p.*?>)(.+?)()";
		//System.out.println(bodies.replaceAll(pattern1, "$2"));
		
	  //  Pattern p = Pattern.compile("(<p>)(</p>)");
	   // Matcher m = p.matcher(body);

	 // print all the matches that we find
//	    while (m.find())
//	    {
//	      System.out.println(m.group(1));
//	    }
		
	}

}
