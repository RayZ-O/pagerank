package com.ufl.ids15.pagerank;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.mahout.text.wikipedia.XmlInputFormat;


public class PageRank {
    private static String numberOfPages;
    private static String initialized = "false";
    private static HashMap<String, String> outPaths;

    public static void initialFiles (String outBucketName) {
	outPaths = new HashMap<String, String>();
	String tmpPathName = outBucketName + "tmp/";
	String resultPathName = outBucketName + "results/";
	outPaths.put("outlinkRes", resultPathName + "PageRank.outlink.out");
	outPaths.put("nRes", resultPathName + "PageRank.n.out");
	outPaths.put("iter1Res", resultPathName + "PageRank.iter1.out");
	outPaths.put("iter8Res", resultPathName + "PageRank.iter8.out");
	outPaths.put("job1Tmp", tmpPathName + "job1/");
	outPaths.put("job2Tmp", tmpPathName + "job2/");
	outPaths.put("job3Tmp", tmpPathName + "job3/");
	outPaths.put("job4Tmp", tmpPathName + "job4/");
	outPaths.put("job5Tmp", tmpPathName + "job5/");
    }

    // Job 1: extract outlink from XML file and remove red link
    public static void parseXml(String input, String output) throws Exception {
	JobConf conf = new JobConf(PageRank.class);
	conf.setJobName("xmlextract");
	conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
	conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

	conf.setInputFormat(XmlInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	conf.setMapperClass(XmlExtractMapper.class);
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(Text.class);
	conf.setReducerClass(XmlExtractReducer.class);

	FileInputFormat.setInputPaths(conf, new Path(input));
	FileOutputFormat.setOutputPath(conf, new Path(output));

	JobClient.runJob(conf);
    }

    // Job 2: Generate adjacency outlink graph
    public static void getAdjacencyGraph(String input, String output) throws Exception {
	JobConf conf = new JobConf(PageRank.class);
	conf.setJobName("outgraphgenerate");

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputValueClass(Text.class);

	conf.setMapperClass(OutGraphGenerateMapper.class);
	conf.setOutputFormat(TextOutputFormat.class);
	conf.setOutputKeyClass(Text.class);
	conf.setReducerClass(OutGraphGenerateReducer.class);

	FileInputFormat.setInputPaths(conf, new Path(input));
	FileOutputFormat.setOutputPath(conf, new Path(output));

	JobClient.runJob(conf);
    }

    // Job3: Count number of pages
    public static void calTotalPages(String input, String output) throws Exception {
	JobConf conf = new JobConf(PageRank.class);
	conf.setJobName("pagecount");

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	conf.setMapperClass(PageCountMapper.class);
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);
	conf.setReducerClass(PageCountReducer.class);

	FileInputFormat.setInputPaths(conf, new Path(input));
	FileOutputFormat.setOutputPath(conf, new Path(output));

	JobClient.runJob(conf);
    }

    // Job4: Calculate PageRank
    public static void calPageRank(String input, String output) throws Exception {
	JobConf conf = new JobConf(PageRank.class);
	conf.setJobName("pagerank");

	conf.set("NumberOfPages", numberOfPages);
	conf.set("Initialized", initialized);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	conf.setMapperClass(PageRankMapper.class);
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(PageRankGenericWritable.class);
	conf.setReducerClass(PageRankReducer.class);

	FileInputFormat.setInputPaths(conf, new Path(input));
	FileOutputFormat.setOutputPath(conf, new Path(output));

	JobClient.runJob(conf);
    }

    // Job 5: Sort by PageRank and write out Pages >= 5/N
    public static void orderRank(String input, String output) throws Exception {
	JobConf conf = new JobConf(PageRank.class);
	conf.setJobName("ranksort");
	conf.set("NumberOfPages", numberOfPages);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	conf.setMapperClass(RankSortMapper.class);
	conf.setOutputKeyComparatorClass(DescDoubleComparator.class);
	conf.setOutputKeyClass(DoubleWritable.class);
	conf.setOutputValueClass(Text.class);
	conf.setReducerClass(RankSortReducer.class);
	conf.setNumReduceTasks(1);

	FileInputFormat.setInputPaths(conf, new Path(input));
	FileOutputFormat.setOutputPath(conf, new Path(output));

	JobClient.runJob(conf);
    }



    public static void main(String[] args) throws Exception {
	// initialization
	// args[0]: input s3://uf-dsr-courses-ids/data/enwiki-latest-pages-articles.xml
	// args[1]: output s3://rui-zhang-emr/
	Configuration conf = new Configuration();
	FileSystem fs =  FileSystem.get(new URI(args[1]), conf);
	PageRank.initialFiles(args[1]);
	//extract wiki and remove redlinks
	PageRank.parseXml(args[0], outPaths.get("job1Tmp"));
	// wiki adjacency graph generation
	PageRank.getAdjacencyGraph(outPaths.get("job1Tmp"), outPaths.get("job2Tmp"));
	FileUtil.copyMerge(fs, new Path(outPaths.get("job2Tmp")), fs, new Path(outPaths.get("outlinkRes")), false, conf, "");
	// total number of pages
	PageRank.calTotalPages(outPaths.get("outlinkRes"), outPaths.get("job3Tmp"));
	FileUtil.copyMerge(fs, new Path(outPaths.get("job3Tmp")), fs, new Path(outPaths.get("nRes")), false, conf, "");
	BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(outPaths.get("nRes")))));
	String line = br.readLine();
	numberOfPages = line.substring(line.indexOf('=') + 1);
	// iterative MapReduce
	PageRank.calPageRank(outPaths.get("outlinkRes"), outPaths.get("job4Tmp") + "iter0");
	initialized = "true";
	for (int i = 0; i < 8; i++) {
	    PageRank.calPageRank(outPaths.get("job4Tmp") + "iter" + i, outPaths.get("job4Tmp") + "iter" + (i + 1));
	}
	// Rank page in the descending order of PageRank
	PageRank.orderRank(outPaths.get("job4Tmp") + "iter1", outPaths.get("job5Tmp") + "iter1");
	PageRank.orderRank(outPaths.get("job4Tmp") + "iter8", outPaths.get("job5Tmp") + "iter8");
	FileUtil.copyMerge(fs, new Path(outPaths.get("job5Tmp") + "iter1"), fs, new Path(outPaths.get("iter1Res")), false, conf, "");
	FileUtil.copyMerge(fs, new Path(outPaths.get("job5Tmp") + "iter8"), fs, new Path(outPaths.get("iter8Res")), false, conf, "");
    }
}


