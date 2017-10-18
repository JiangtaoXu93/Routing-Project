package org.neu.mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.neu.data.FlightData;
import org.neu.data.RouteKey;
import org.neu.util.DataUtil;


public class RouteComputeMapper extends
    Mapper<LongWritable, Text, RouteKey, FlightData> {

  private static Set<String> sourceSet = new HashSet<>();
  private static Set<String> destinationSet = new HashSet<>();
  private static Set<Integer> yearSet = new HashSet<>();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    computeQueryData(context);
  }

  private void computeQueryData(Context context) throws IOException {
    if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
      URI mappingFileUri = context.getCacheFiles()[0];
      if (mappingFileUri != null) {
        processQueryData(context.getConfiguration(), mappingFileUri);
      } else {
        System.out.println(">>>>>> NO MAPPING FILE");
      }
    } else {
      System.out.println(">>>>>> NO CACHE FILES AT ALL");
    }
  }

  private void processQueryData(Configuration conf, URI mappingFileUri)
      throws IOException {
    FileSystem fs = FileSystem.get(mappingFileUri, conf);
    FileStatus[] status = fs.listStatus(new Path(mappingFileUri));
    InputStreamReader inputStreamReader = new InputStreamReader(fs.open(
        status[0].getPath()));
    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      String[] values = line.split(",");
      yearSet.add(Integer.valueOf(values[0]));
      sourceSet.add(values[3]);
      destinationSet.add(values[4]);
    }
    inputStreamReader.close();
    bufferedReader.close();
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    FlightData fd = DataUtil.getFlightData(value);



  }
}
