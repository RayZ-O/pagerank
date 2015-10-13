package com.ufl.ids15.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PageRankReducer extends MapReduceBase implements
	Reducer<Text, PageRankGenericWritable, Text, Text> {

    static final double d = 0.85;
    private Text adjacency = new Text();
    private DoubleWritable rank = null;
    private Text outputValue = new Text();

    private static long N;
    private static double factor;
    private static String initialized;
    @Override
    public void configure(JobConf job) {
	N = Long.parseLong(job.get("NumberOfPages"));
	factor = (1 - d) / N;
	initialized = job.get("Initialized");
    }

    @Override
    public void reduce(Text key, Iterator<PageRankGenericWritable> values,
	    OutputCollector<Text, Text> output,
	    Reporter reporter) throws IOException {

	double newRank = 0.0;
	adjacency.set("");
	while (values.hasNext()) {
	    Writable val = values.next().get();
	    if (val instanceof DoubleWritable) {
		rank = (DoubleWritable) val;
		newRank += rank.get();
	    } else {
		adjacency.set((Text) val);
	    }
	}
	String rank = initialized.equals("true") ? String.valueOf(factor + d * newRank) : String.valueOf(1.0 / N);
	 outputValue.set(rank + adjacency);
	output.collect(key, outputValue);
    }
}
