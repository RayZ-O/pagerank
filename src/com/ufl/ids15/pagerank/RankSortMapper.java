package com.ufl.ids15.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class RankSortMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, DoubleWritable, Text> {

    private DoubleWritable rank = new DoubleWritable();
    private Text title = new Text();

    @Override
    public void map(LongWritable key, Text value,
	    OutputCollector<DoubleWritable, Text> output, Reporter reporter)
             throws IOException {
	String line = value.toString();
	String[] s = line.split("\t", 3);
	title.set(s[0]);
	rank.set(Double.parseDouble(s[1]));
	output.collect(rank, title);
    }

}
