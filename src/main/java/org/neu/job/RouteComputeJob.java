package org.neu.job;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.neu.data.FlightData;
import org.neu.data.RouteData;
import org.neu.data.RouteKey;
import org.neu.mapper.RouteComputeMapper;
import org.neu.reducer.RouteComputeReducer;

public class RouteComputeJob extends Configured implements Tool {

  private static String OUTPUT_SEPARATOR = "mapreduce.output.textoutputformat.separator";


  @Override
  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "RouteComputeJob");
    job.setJarByClass(this.getClass());
    job.getConfiguration().set(OUTPUT_SEPARATOR, ",");

    FileInputFormat.addInputPath(job, new Path(args[2]));
    FileOutputFormat.setOutputPath(job, new Path(args[3] + "/route"));
    job.addCacheFile(new Path(args[1] + "/query.csv").toUri());

    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

    job.setMapperClass(RouteComputeMapper.class);
    job.setReducerClass(RouteComputeReducer.class);

    job.setMapOutputKeyClass(RouteKey.class);
    job.setMapOutputValueClass(FlightData.class);
    job.setOutputKeyClass(RouteKey.class);
    job.setOutputValueClass(RouteData.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }
}
