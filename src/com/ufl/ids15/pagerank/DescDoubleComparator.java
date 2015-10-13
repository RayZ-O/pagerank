package com.ufl.ids15.pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DescDoubleComparator extends WritableComparator {

    protected DescDoubleComparator() {
	super(DoubleWritable.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
	DoubleWritable d1 = (DoubleWritable) w1;
	DoubleWritable d2 = (DoubleWritable) w2;
	return -d1.compareTo(d2);
    }

}
