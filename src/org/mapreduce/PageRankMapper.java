package org.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PageRankMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {	
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	private String[] getLinksList(String line) {
		return line.split("\t");
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String line = value.toString();
		int separateIndex = line.indexOf("\t");
		String current_link = line.substring(0, separateIndex);
		String rest = line.substring(separateIndex, line.length());
		String[] valuesList = getLinksList(rest);
		String pagerank = valuesList[1];
		
		StringBuilder sb = new StringBuilder();
		outputValue.set(pagerank + "\t" + current_link + "\t" + (valuesList.length-1) + "\t");
		for (int i = 2; i < valuesList.length; i++) {
			outputKey.set(valuesList[i]);
			output.collect(outputKey, outputValue);
			sb.append(valuesList[i] + "\t");
		}
		
		output.collect(new Text(current_link), new Text("\t\t" + sb.toString()));
	}
}
