package com.ufl.ids15.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PageCountReducer extends MapReduceBase implements
	Reducer<Text, IntWritable, Text, NullWritable> {

    @Override
    public void reduce(Text key, Iterator<IntWritable> values,
	    OutputCollector<Text, NullWritable> output, Reporter reporter)
	    throws IOException {
	long sum = 0;
	while (values.hasNext()) {
	    sum += values.next().get();
	}
	output.collect(new Text("N=" + sum), null);
    }

}
