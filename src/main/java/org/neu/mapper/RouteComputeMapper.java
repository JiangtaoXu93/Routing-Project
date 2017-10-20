package org.neu.mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
  private static List<String[]> queryList = new ArrayList<>();
  private static Set<Integer> yearSet = new HashSet<>();
  private static int computeYear;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    DataUtil.initCsvColumnMap();
    loadQueryData(context);
    computeYear = yearSet.iterator().next();// Assuming we have only one year in all queries
  }

  private void loadQueryData(Context context) throws IOException {
    if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
      URI mappingFileUri = context.getCacheFiles()[0];
      if (mappingFileUri != null) {
        getQueryData(context.getConfiguration(), mappingFileUri);
      } else {
        System.out.println(">>>>>> NO MAPPING FILE");
      }
    } else {
      System.out.println(">>>>>> NO CACHE FILES AT ALL");
    }
  }

  private void getQueryData(Configuration conf, URI mappingFileUri)
      throws IOException {
    FileSystem fs = FileSystem.get(mappingFileUri, conf);
    FileStatus[] status = fs.listStatus(new Path(mappingFileUri));
    InputStreamReader inputStreamReader = new InputStreamReader(fs.open(
        status[0].getPath()));
    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      String[] values = line.split(",");

      /*Add Year*/
      yearSet.add(Integer.valueOf(values[0]));

      /*Load SD MAP*/
      Map<String, Integer> dMap = sdMap.getOrDefault(values[3], new HashMap<>());
      dMap.put(values[4], dMap.getOrDefault(values[4], 0) + 1);
      sdMap.put(values[3], dMap);

      /*Load DS MAP*/
      Map<String, Integer> sMap = dsMap.getOrDefault(values[4], new HashMap<>());
      sMap.put(values[3], sMap.getOrDefault(values[3], 0) + 1);
      dsMap.put(values[4], sMap);

      /*Add Query*/
      queryList.add(values);
    }
    inputStreamReader.close();
    bufferedReader.close();
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    FlightData fd = DataUtil.getFlightData(value);
    if (null != fd) {
      emitTrainData(context, fd);
      emitTestData(context, fd);
    }
  }

  private void emitTestData(Context context, FlightData fd)
      throws IOException, InterruptedException {
    for (String[] query : queryList) {
      String flightDate = getFlightDate(fd);
      String queryDate = query[0] + query[1] + query[2];
      String queryOrigin = query[3];
      String queryDes = query[4];
      String queryNextDate = getNextDate(queryDate);

      if (StringUtils.equals(queryDate, flightDate)) {
        //Emit Test LegOne
        if (StringUtils.equals(queryOrigin, fd.getOrigin().toString())) {
          fd.setLegType(new IntWritable(1));
          RouteKey rk = new RouteKey(fd.getOrigin(), fd.getDest(), new Text(queryDes),
              new IntWritable(2), new Text(queryDate));
          context.write(rk, fd);
        }

        //Emit Test LegTwo
        if (StringUtils.equals(queryDes, fd.getDest().toString())) {
          fd.setLegType(new IntWritable(2));
          RouteKey rk = new RouteKey(new Text(queryOrigin), fd.getOrigin(), fd.getDest(),
              new IntWritable(2), new Text(queryDate));
          context.write(rk, fd);
        }
      }

      if (StringUtils.equals(queryNextDate, flightDate)) {
        //Emit Test LegTwo
        if (StringUtils.equals(queryDes, fd.getDest().toString())) {
          fd.setLegType(new IntWritable(2));
          RouteKey rk = new RouteKey(new Text(queryOrigin), fd.getOrigin(), fd.getDest(),
              new IntWritable(2), new Text(queryDate));
          context.write(rk, fd);
        }
      }

    }
  }

  private String getFlightDate(FlightData fd) {
    return fd.getYear().toString()
        + StringUtils.leftPad(fd.getMonth().toString(), 2, '0')
        + StringUtils.leftPad(fd.getDayOfMonth().toString(), 2, '0');
  }

  private String getNextDate(String dt) {
    try {
      String qDt = dt;
      SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
      Calendar c = Calendar.getInstance();
      c.setTime(sdf.parse(dt));
      c.add(Calendar.DATE, 1);  // number of days to add
      dt = sdf.format(c.getTime());
      return qDt;
    } catch (ParseException pe) {
      return dt;
    }
  }

  private void emitTrainData(Context context, FlightData fd)
      throws IOException, InterruptedException {
    //TODO: Change (computeYear - 25) -> (computeYear - 5)
    if (fd.getYear().get() >= (computeYear - 25) && fd.getYear().get() < computeYear) {
      writeLegOneTrainFlight(context, fd);
      writeLegTwoTrainFlight(context, fd);
    }
  }

  private void writeLegOneTrainFlight(Context context, FlightData fd)
      throws IOException, InterruptedException {
    fd.setLegType(new IntWritable(1));
    for (Map.Entry<String, Map<String, Integer>> entry : sdMap.entrySet()) {
      if (StringUtils.equals(entry.getKey(), fd.getOrigin().toString())) {
        for (String des : entry.getValue().keySet()) {
          RouteKey rk = new RouteKey(fd.getOrigin(), fd.getDest(), new Text(des),
              new IntWritable(1), new Text(getFlightDate(fd)));
          context.write(rk, fd);
        }
      }
    }
  }

  private void writeLegTwoTrainFlight(Context context, FlightData fd)
      throws IOException, InterruptedException {
    fd.setLegType(new IntWritable(2));
    for (Map.Entry<String, Map<String, Integer>> entry : dsMap.entrySet()) {
      if (StringUtils.equals(entry.getKey(), fd.getDest().toString())) {
        for (String origin : entry.getValue().keySet()) {
          RouteKey rk = new RouteKey(new Text(origin), fd.getOrigin(), fd.getDest(),
              new IntWritable(1), new Text(getFlightDate(fd)));
          context.write(rk, fd);
        }
      }
    }
  }
}