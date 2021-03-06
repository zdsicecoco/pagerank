package org.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class LinkGraphDriver {

	public static JobConf createJobConf(String input_dir, String output_dir) {
		JobConf conf = new JobConf(org.mapreduce.LinkGraphDriver.class);
		conf.setMapperClass(org.mapreduce.LinkGraphMapper.class);
		conf.setReducerClass(org.mapreduce.LinkGraphReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.set("pagerank.dampfactor", String.valueOf(0.85));

		FileInputFormat.addInputPath(conf, new Path(input_dir));
		FileOutputFormat.setOutputPath(conf, new Path(output_dir));

		return conf;
	}
	
	public static void main(String[] args) throws IOException {
		JobClient client = new JobClient();

		JobConf conf = new JobConf(org.mapreduce.LinkGraphDriver.class);
		conf.setMapperClass(org.mapreduce.LinkGraphMapper.class);
		conf.setReducerClass(org.mapreduce.LinkGraphReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.set("pagerank.dampfactor", String.valueOf(0.85));

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		client.setConf(conf);
		JobClient.runJob(conf);
	}
}
