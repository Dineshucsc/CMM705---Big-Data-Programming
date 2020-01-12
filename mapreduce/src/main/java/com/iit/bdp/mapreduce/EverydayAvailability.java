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
import java.util.Map;

public class EverydayAvailability {


    public static class EverydayAvailabilityMapper extends
            Mapper<Object, Text, NullWritable, NullWritable> {

        public static final String AVAIL_COUNTER_GROUP = "EverydayAvailability";


        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            Map<String, String> parsed = Util.transAvailability(value
                    .toString());

            String opendays = parsed.get("availability");
            System.out.println("opendays :" + opendays);

            if (opendays != null && !opendays.isEmpty()) {

                if (opendays.equalsIgnoreCase("365")) {
                    context.getCounter(AVAIL_COUNTER_GROUP, opendays)
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
            System.err.println("input: 365 availability <in> <out>");
            System.exit(2);
        }

        Path input = new Path(otherArgs[0]);
        Path outputDir = new Path(otherArgs[1]);

        Job job = Job.getInstance(conf, "365 availability");
        job.setJarByClass(EverydayAvailability.class);

        job.setMapperClass(EverydayAvailability.EverydayAvailabilityMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);

        int code = job.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {
            for (Counter counter : job.getCounters().getGroup(
                    EverydayAvailabilityMapper.AVAIL_COUNTER_GROUP)) {
                System.out.println("Total number of rentals that are available 365 days a year :" + "\t"
                        + counter.getValue());
            }
        }

        // Clean up empty output directory
        FileSystem.get(conf).delete(outputDir, true);

        System.exit(code);
    }
}
