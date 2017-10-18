package org.neu.mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.neu.data.FlightData;
import org.neu.data.RouteKey;
import org.neu.util.DataUtil;


public class RouteComputeMapper extends
    Mapper<LongWritable, Text, RouteKey, FlightData> {

  private static Map<String, Map<String, Integer>> sdMap = new HashMap<>();
  private static Map<String, Map<String, Integer>> dsMap = new HashMap<>();
  private static Set<Integer> yearSet = new HashSet<>();
  private static int computeYear;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    DataUtil.initCsvColumnMap();
    computeQueryData(context);
    computeYear = yearSet.iterator().next();// Assuming we have only one year in all queries
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

      Map<String, Integer> dMap = sdMap.getOrDefault(values[3], new HashMap<>());
      dMap.put(values[4], dMap.getOrDefault(values[4], 0) + 1);
      sdMap.put(values[3], dMap);

      Map<String, Integer> sMap = dsMap.getOrDefault(values[4], new HashMap<>());
      sMap.put(values[3], sMap.getOrDefault(values[3], 0) + 1);
      dsMap.put(values[4], sMap);

    }
    inputStreamReader.close();
    bufferedReader.close();
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    FlightData fd = DataUtil.getFlightData(value);
    if (null != fd && fd.getYear().get() >= (computeYear - 5) && fd.getYear().get() < computeYear) {
      writeLegOneFlight(context, fd);
      writeLegTwoFlight(context, fd);
    }
  }

  private void writeLegOneFlight(Context context, FlightData fd)
      throws IOException, InterruptedException {
    fd.setLegType(new IntWritable(1));
    for (Map.Entry<String, Map<String, Integer>> entry : sdMap.entrySet()) {
      if (StringUtils.equals(entry.getKey(), fd.getOrigin().toString())) {
        for (String des : entry.getValue().keySet()) {
          RouteKey rk = new RouteKey(fd.getOrigin(), fd.getDest(), new Text(des));
          context.write(rk, fd);
        }
      }
    }
  }

  private void writeLegTwoFlight(Context context, FlightData fd)
      throws IOException, InterruptedException {
    fd.setLegType(new IntWritable(2));
    for (Map.Entry<String, Map<String, Integer>> entry : dsMap.entrySet()) {
      if (StringUtils.equals(entry.getKey(), fd.getDest().toString())) {
        for (String origin : entry.getValue().keySet()) {
          RouteKey rk = new RouteKey(new Text(origin), fd.getOrigin(), fd.getDest());
          context.write(rk, fd);
        }
      }
    }
  }
}
