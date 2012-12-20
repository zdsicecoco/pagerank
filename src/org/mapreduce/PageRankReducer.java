package org.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PageRankReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	private double dampFactor; 
	private Text outputValue = new Text();
	
	public void configure(JobConf job) {
		dampFactor = Double.valueOf(job.get("pagerank.dampfactor"));
	}
	
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException { 
		double sum = 0;
		String outlinks = "";
		while (values.hasNext()) {
			String temp = values.next().toString();
						
			if (temp.indexOf("\t\t") == 0) {
				outlinks = temp.substring(temp.indexOf("\t\t")+2, temp.length());
			}
			else {
				String[] array = temp.split("\t");
				sum += Double.valueOf(array[0])/Double.valueOf(array[2]);
			}
		}
		
		sum = dampFactor * sum + (1 - dampFactor);
		outputValue.set(String.valueOf(sum) + "\t" + outlinks);

		output.collect(key, outputValue);
	}
}
