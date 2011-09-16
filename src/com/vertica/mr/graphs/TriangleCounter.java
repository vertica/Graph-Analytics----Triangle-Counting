package com.vertica.mr.graphs;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

// M/R code to count the # of triangles in a graph.
public class TriangleCounter extends Configured implements Tool
{
    // Maps values to Long,Long pairs. 
    public static class ParseLongLongPairsMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable>
    {
        LongWritable mKey = new LongWritable();
        LongWritable mValue = new LongWritable();

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            long e1,e2;
            if (tokenizer.hasMoreTokens()) {
                e1 = Long.parseLong(tokenizer.nextToken());
                if (!tokenizer.hasMoreTokens())
                    throw new RuntimeException("invalid edge line " + line);
                e2 = Long.parseLong(tokenizer.nextToken());
                // Input contains reciprocal edges, only need one.
                if (e1 < e2) {
                    mKey.set(e1);
                    mValue.set(e2);
                    context.write(mKey,mValue);
                }
            }
        }
    }

    // Produces original edges and triads.
    public static class TriadsReducer extends Reducer<LongWritable, LongWritable, Text, LongWritable>
    {
        Text rKey = new Text();
        final static LongWritable zero = new LongWritable((byte)0);
        final static LongWritable one = new LongWritable((byte)1);
        long []vArray = new long[4096];
        int size = 0;

        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException
        {
            // Produce triads - all permutations of pairs where e1 < e2 (value=1).
            // And all original edges (value=0).
            // Sorted by value.
            Iterator<LongWritable> vs = values.iterator();
            for (size = 0; vs.hasNext(); ) {
                if (vArray.length==size) {
                    vArray = Arrays.copyOf(vArray, vArray.length*2);
                }

                long e = vs.next().get();
                vArray[size++] = e;

                // Original edge.
                rKey.set(key.toString() + "," + Long.toString(e));
                context.write(rKey, zero);
            }

            Arrays.sort(vArray, 0, size);

            // Generate triads.
            for (int i=0; i<size; ++i) {
                for (int j=i+1; j<size; ++j) {
                    rKey.set(Long.toString(vArray[i]) + "," + Long.toString(vArray[j]));
                    context.write(rKey, one);
                }
            }
        }
    }

    // Parses values into {Text,Long} pairs.
    public static class ParseTextLongPairsMapper extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        Text mKey = new Text();
        LongWritable mValue = new LongWritable();

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            if (tokenizer.hasMoreTokens()) {
                mKey.set(tokenizer.nextToken());
                if (!tokenizer.hasMoreTokens())
                    throw new RuntimeException("invalid intermediate line " + line);
                mValue.set(Long.parseLong(tokenizer.nextToken()));
                context.write(mKey, mValue);
            }
        }
    }

    // Counts the number of triangles.
    public static class CountTrianglesReducer extends Reducer<Text, LongWritable, LongWritable, LongWritable>
    {
        long count = 0;
        final static LongWritable zero = new LongWritable(0);

        public void cleanup(Context context)
            throws IOException, InterruptedException
        {
            LongWritable v = new LongWritable(count);
            if (count > 0) context.write(zero, v);
        }

        public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException
        {
            boolean isClosed = false;
            long c = 0, n = 0;
            Iterator<LongWritable> vs = values.iterator();
            // Triad edge value=1, original edge value=0.
            while (vs.hasNext()) {
                c += vs.next().get();
                ++n;
            }
            if (c!=n) count += c;
        }
    }

    // Aggregates the counts.
    public static class AggregateCountsReducer extends Reducer<Text, LongWritable, LongWritable, LongWritable>
    {
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException
        {
            long sum = 0;
            Iterator<LongWritable> vs = values.iterator();
            while (vs.hasNext()) {
                sum += vs.next().get();
            }
            context.write(new LongWritable(sum), null);
        }
    }

    // Takes two arguments, the edges file and output file. Edges must be reciprocal, that is every
    // {source, dest} edge must have a corresponding {dest, source}.
    // File must be of the form:
    //   long <whitespace> long <newline>
    //   <repeat>
    public int run(String[] args) throws Exception
    {
        Job job1 = new Job(getConf());
        job1.setJobName("triads");

        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(LongWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        job1.setJarByClass(TriangleCounter.class);
        job1.setMapperClass(ParseLongLongPairsMapper.class);
        job1.setReducerClass(TriadsReducer.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp1"));


        Job job2 = new Job(getConf());
        job2.setJobName("triangles");

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);

        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(LongWritable.class);

        job2.setJarByClass(TriangleCounter.class);
        job2.setMapperClass(ParseTextLongPairsMapper.class);
        job2.setReducerClass(CountTrianglesReducer.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job2, new Path("temp1"));
        FileOutputFormat.setOutputPath(job2, new Path("temp2"));


        Job job3 = new Job(getConf());
        job3.setJobName("count");

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(LongWritable.class);

        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(LongWritable.class);

        job3.setJarByClass(TriangleCounter.class);
        job3.setMapperClass(ParseTextLongPairsMapper.class);
        job3.setReducerClass(AggregateCountsReducer.class);

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job3, new Path("temp2"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));


        int ret = job1.waitForCompletion(true) ? 0 : 1;
        if (ret==0) ret = job2.waitForCompletion(true) ? 0 : 1;
        if (ret==0) ret = job3.waitForCompletion(true) ? 0 : 1;
        return ret;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TriangleCounter(), args);
        System.exit(res);
    }
}
