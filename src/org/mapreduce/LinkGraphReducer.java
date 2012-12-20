package org.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class LinkGraphReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	private double dampFactor; 
	
	public void configure(JobConf job) {
		dampFactor = Double.valueOf(job.get("pagerank.dampfactor"));
	}

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException { 
		StringBuilder sb = new StringBuilder();
		sb.append(String.valueOf(1-dampFactor) + "\t");
		while (values.hasNext()) {
			sb.append(values.next().toString() + "\t");
		}
		Text outputValue = new Text();

		outputValue.set(sb.toString());
		output.collect(key, outputValue);
	}
}
