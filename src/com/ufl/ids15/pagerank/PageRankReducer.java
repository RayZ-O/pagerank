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

import com.ufl.ids15.pagerank.PageRank.Counter;

public class PageRankReducer extends MapReduceBase implements
	Reducer<Text, PageRankGenericWritable, Text, Text> {

    static final double d = 0.85;
    private Text adjacency = new Text();
    private Text outputValue = new Text();
    private DoubleWritable rank = null;

    private static long N;
    private static double factor;
    private static String initialized;
    private static double epsilon;
    @Override
    public void configure(JobConf job) {
	N = Long.parseLong(job.get("NumberOfPages"));
	factor = (1 - d) / N;
	initialized = job.get("Initialized");
	epsilon = Double.parseDouble(job.get("Epsilon"));
    }

    @Override
    public void reduce(Text key, Iterator<PageRankGenericWritable> values,
	    OutputCollector<Text, Text> output,
	    Reporter reporter) throws IOException {
	if (initialized.equals("false")) {
	    output.collect(key, new Text(String.valueOf(1.0 / N) + values.next().get()));
	    return;
	}
	double newRank = 0.0;
	double lastRank = 0.0;
	adjacency.set("");
	while (values.hasNext()) {
	    Writable val = values.next().get();
	    if (val instanceof DoubleWritable) {
		rank = (DoubleWritable) val;
		newRank += rank.get();
	    } else {
		String valStr = ((Text)val).toString();
		if (valStr.startsWith("#")) {
		    lastRank = Double.parseDouble(valStr.substring(1));
		} else {
		    adjacency.set((Text)val);
		}
	    }
	}
	newRank = factor + d * newRank;
	String rank = String.valueOf(newRank);
	if (Math.abs(newRank - lastRank) <= epsilon) {
	    reporter.getCounter(Counter.CONVERGENCE_COUNTER).increment(1);
	}
	outputValue.set(rank + adjacency);
	output.collect(key, outputValue);
    }
}
