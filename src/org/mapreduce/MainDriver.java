package org.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class MainDriver {
	
	public static void deleteDir(String uri) throws IOException {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		Path path = new Path(uri);
		System.out.println(path.toString());
		hdfs.delete(path, true);
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: PageRank <inDir> <outDir>");
			System.exit(1);
		}
		String input_dir = args[0];
		String output_dir = args[1];
		String tempDir = "temp";
		int iters = 5;
		int reducers = 1;

		/*Step 1. Link Graph Generation*/
		System.out.println("[building the link graph]");
		String linkgraph_dir = tempDir;
		deleteDir(linkgraph_dir);
		JobConf linkConf = org.mapreduce.LinkGraphDriver.createJobConf(input_dir, linkgraph_dir);
		JobClient.runJob(linkConf);
		System.out.println("done with building the link graph...");

		/*Step 2. Map Reduce to Page Rank*/
		System.out.println("[doing page rank]");
		String iteration_input = linkgraph_dir;
		String iteration_output = output_dir;
		deleteDir(iteration_output);
		for (int i = 0; i < iters; i++) {
			System.out.println("iteration " + i);
			JobConf iterationConf = org.mapreduce.PageRankDriver.createJobConf(iteration_input, iteration_output, reducers);
			JobClient.runJob(iterationConf);
			String temp = iteration_input;
			iteration_input = iteration_output;
			iteration_output = temp;
			deleteDir(iteration_output);
		}
		System.out.println("[done with the page rank iteration]");

		/*Step 3. Sort the rank*/
		System.out.println("[sorting the page rank results]");
		String sort_input = iteration_input;
		String sort_output = iteration_output;
		deleteDir(sort_output);
		JobConf sortConf = org.mapreduce.SortDriver.createJobConf(sort_input, sort_output, reducers);
		JobClient.runJob(sortConf);
		
		System.out.println("[copy the result form FS to local file system]");
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		Path srcPath = new Path(sort_output);
		Path dstPath = new Path(output_dir);
		hdfs.copyToLocalFile(srcPath, dstPath);
		System.out.println("Done with page rank sorting");
		
		/* Extract the top 50 pages form the result */
	    BufferedReader reader = new BufferedReader(new FileReader (dstPath+"/part-00000"));
	    ArrayList<String> top50 = new ArrayList<String>();
	    String line = null;
	    while((line = reader.readLine()) != null)
	        top50.add(line);
	    
		BufferedWriter out = new BufferedWriter(new FileWriter(dstPath+"/top50pages.txt"));
		for (int i=top50.size()-1; i>top50.size()-51; i--) {
//			System.out.println(i+top50.size());
			out.write(top50.get(i));
			out.newLine();
		}
		out.close();
		
		System.out.println("*** Finished the page rank! Check the result in " + output_dir+"/top50pages.txt ***");
		
		System.exit(0);
	}
}
