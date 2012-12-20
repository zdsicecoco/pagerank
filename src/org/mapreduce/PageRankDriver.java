package org.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class PageRankDriver {

	public static JobConf createJobConf(String input_dir, String output_dir, int reducer_num) {
		JobConf conf = new JobConf(org.mapreduce.PageRankDriver.class);
		conf.setMapperClass(org.mapreduce.PageRankMapper.class);
		conf.setReducerClass(org.mapreduce.PageRankReducer.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setNumReduceTasks(reducer_num);
		conf.set("pagerank.dampfactor", String.valueOf(0.85));

		FileInputFormat.addInputPath(conf, new Path(input_dir));
		FileOutputFormat.setOutputPath(conf, new Path(output_dir));

		return conf;
	}
	
	public static void deleteDir(String uri) throws IOException {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		Path path = new Path(uri);
		System.out.println(path.toString());
		hdfs.delete(path, true);
	}
	
	public static void main(String[] args) throws IOException {
		String intermediaDir = args[1];
		String outputDir = args[2];

		String iteration_input = intermediaDir;
		String iteration_output = outputDir;
		deleteDir(iteration_output);

		int iter_num = 5;
		for (int i = 0; i < iter_num; i++) {
			System.out.println("################## iteration " + i+ " ##################");
			JobConf conf = createJobConf(iteration_input, iteration_output, 1);
			JobClient.runJob(conf);
			String temp = iteration_input;
			iteration_input = iteration_output;
			iteration_output = temp;
			deleteDir(iteration_output);
		}
	}
}
