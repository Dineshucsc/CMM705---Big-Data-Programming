package com.iit.bdp.mapreduce;

import com.iit.bdp.mapreduce.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;
public class NeighbourhoodGroup {

    public static class NeighbourhoodGroupMapper extends
            Mapper<Object, Text, NullWritable, NullWritable> {

        public static final String REGION_COUNTER_GROUP = "NeighbourhoodGroup";

        static List<String> regions = Arrays.asList( new String[]{"Central Region","North Region","East Region","West Region","North-East Region"});

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {


            Map<String, String> parsed = Util.transRegeion(value
                    .toString());


            String region = parsed.get("region");
            System.out.println("region :" + region);


            if (region != null && !region.isEmpty()) {

                if(regions.contains(region)){
                    context.getCounter(REGION_COUNTER_GROUP, region)
                            .increment(1);
                }

            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("input: NeighbourhoodGroup <in> <out>");
            System.exit(2);
        }

        Path input = new Path(otherArgs[0]);
        Path outputDir = new Path(otherArgs[1]);

        Job job = Job.getInstance(conf, "Count by Neighbourhood Group");
        job.setJarByClass(NeighbourhoodGroup.class);

        job.setMapperClass(NeighbourhoodGroup.NeighbourhoodGroupMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);

        int code = job.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {
            for (Counter counter : job.getCounters().getGroup(
                    NeighbourhoodGroup.NeighbourhoodGroupMapper.REGION_COUNTER_GROUP)) {
                System.out.println(counter.getDisplayName() + "\t"
                        + counter.getValue());
            }
        }

        // Clean up empty output directory
        FileSystem.get(conf).delete(outputDir, true);

        System.exit(code);
    }
}
