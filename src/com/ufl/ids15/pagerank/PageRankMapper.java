package com.ufl.ids15.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PageRankMapper extends MapReduceBase implements
Mapper<LongWritable, Text, Text, PageRankGenericWritable> {

    private Text title = new Text();
    private Text adjacency = new Text();
    private Text adjNode = new Text();
    private DoubleWritable rankWritable = new DoubleWritable(0.0);

    private static String initialized;
    @Override
    public void configure(JobConf job) {
	initialized = job.get("Initialized");
    }

    @Override
    public void map(LongWritable key, Text value,
	    OutputCollector<Text, PageRankGenericWritable> output,
	    Reporter reporter) throws IOException {
	String line = value.toString();

	int adjBegin = initialized.equals("false") ? 1 : 2;
	String[] parts = line.split("\t", adjBegin + 1);

	if (parts.length > 2) {
	    String[] adjArray = parts[2].split("\t");
	    long adjListSize = adjArray.length;
	    double rank = adjListSize > 0 ? Double.parseDouble(parts[1]) / adjListSize : 0;
	    for (int i = 0; i < adjListSize; i++) {
		adjNode.set(adjArray[i]);
		rankWritable.set(rank);
		output.collect(adjNode, new PageRankGenericWritable(rankWritable));
	    }
	}
	title.set(parts[0]);
	adjacency.set(parts.length > adjBegin ? '\t' + parts[adjBegin] : "");
	output.collect(title, new PageRankGenericWritable(adjacency));
    }
}
