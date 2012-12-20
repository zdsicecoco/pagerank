package org.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SortMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {	
	private Text outputKey = new Text();
	private Text outputValue = new Text();

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String line = value.toString();
		System.out.println(line);
		String[] valuesList = line.split("\t");
		if (!valuesList[0].equals("\t")) {
			outputKey.set(valuesList[1]);
			outputValue.set(valuesList[0]);
//			System.out.println(valuesList[1]+": "+valuesList[0]);
			output.collect(outputKey, outputValue);
		}
	}
}
