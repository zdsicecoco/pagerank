package org.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class LinkGraphMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	private Text docName = new Text();
	private Text linkName = new Text();	
	
	private List<String> getLinks(String line) {
		List<String> linkslist = new ArrayList<String>();
		
		Pattern pattern = Pattern.compile("\\[\\[(.*?)\\]\\]");
		Matcher m = pattern.matcher(line);
		while (m.find()) {
		    String s = m.group(1);
		    if (s.indexOf("|") != -1)
		    	s = s.substring(0, s.indexOf("|"));
		    linkslist.add(s);
		}
		return linkslist;
	}
	
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {		
		String line = value.toString();
		
		int title_begin = line.indexOf("<title>") + 7;
		int title_end = line.indexOf("</title>");
		String outputKey = line.substring(title_begin, title_end);
//		System.out.println(outputKey);
		
		docName.set(outputKey);
		
		List<String> linkslist = getLinks(line);
		for (int i = 0; i < linkslist.size(); i++) {
			linkName.set(linkslist.get(i));
			output.collect(docName, linkName);
		}
	}
}
