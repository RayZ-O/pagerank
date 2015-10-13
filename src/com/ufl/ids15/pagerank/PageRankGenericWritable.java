package com.ufl.ids15.pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

@SuppressWarnings("unchecked")
public class PageRankGenericWritable extends GenericWritable {

    private static Class<? extends Writable>[] CLASSES = null;

    static {
	CLASSES = new Class[] { Text.class, DoubleWritable.class };
    }

    // this empty initialize is required by Hadoop
    public PageRankGenericWritable() {
    }

    public PageRankGenericWritable(Writable instance) {
	set(instance);
    }

    @Override
    protected Class<? extends Writable>[] getTypes() {
	return CLASSES;
    }

    @Override
    public String toString() {
	return this.get().toString();
    }
}
