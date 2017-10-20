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

/**
 * RouteComputeMapper:
 * Input: Data Files as CSVs are read line by line
 * Key: LongWritable -> Ignored
 * Value: Text -> Actual CSV row
 *
 * For Query A->B
 * Let, Route be A->C->B
 * Then, LegOne Flight : A->C and LegTwo Flight : C->B
 *
 * Output: We emit multiple keys for the same flight.
 * For Train Data (Query Year - 1),
 * - Emit1: LegOne Flight
 * - Emit2: LegTwo Flight
 *
 * For Test Data (Same date as mentioned in given input queries),
 * - Emit1: LegOne Flight (for given Query Date)
 * - Emit2: LegTwo Flight (for given Query Date)
 * - Emit3: LegTwo Flight (Assuming this LegTwo arrived on the next date of the given query date)
 *
 * All the emits are of the form
 * Key : RouteKey
 * Value: FlightData
 *
 * @author Bhanu, Joyal, Jiangtao
 */
public class RouteComputeMapper extends
    Mapper<LongWritable, Text, RouteKey, FlightData> {

  //key: source airport from query; value: set of destination airport from query
  private static Map<String, Set<String>> desToSrcMap = new HashMap<>();
  //key: destination airport from query; value: set of source airport from query
  private static List<String[]> queryList = new ArrayList<>();//the input query
  private static Map<String, Set<String>> srcToDesMap = new HashMap<>();
  private static Set<Integer> yearSet = new HashSet<>();//years of input query
  private static int computeYear;//year of input query

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    DataUtil
        .initCsvColumnMap();//Init DataUtil, which will be used to fetch flight data after sanity
    loadQueryData(context);
    computeYear = yearSet.iterator()
        .next();// Assuming that we have the same year in all queries(which mentioned on piazza)
  }


  /**
   * load query data from cache file
   */
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

  /**
   * go throw each query, and add source, destination airport into srcToDesMap, desToSrcMap
   */
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

      if (!validateInputQuery(values)) {
        continue;
      }

      yearSet.add(Integer.valueOf(values[0]));

      /*Load source to destination MAP*/
      Set<String> destinationSet = srcToDesMap.getOrDefault(values[3], new HashSet<String>());
      destinationSet.add(values[4]);
      srcToDesMap.put(values[3], destinationSet);

      /*Load destination to source MAP*/
      Set<String> sourceSet = desToSrcMap.getOrDefault(values[4], new HashSet<String>());
      sourceSet.add(values[3]);
      desToSrcMap.put(values[4], sourceSet);

      queryList.add(values);
    }
    inputStreamReader.close();
    bufferedReader.close();
  }

  private boolean validateInputQuery(String[] values) {
    if (values.length != 5) {
      return false;
    }
    if (StringUtils.equals(values[3], values[4])) {
      return false;
    }
    values[1] = StringUtils.leftPad(values[1], 2, '0');//add 0 before one digit input of month
    values[2] = StringUtils
        .leftPad(values[2], 2, '0');//add 0 before one digit input of day of month
    return true;
  }

  /**
   * map: generate 2 kind of <key,value>, according to the year, emit testing data in that year
   * and training data not in that year
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    FlightData fd = DataUtil.getFlightData(value);//get flight data
    if (null != fd) {
      emitTrainData(context, fd);//generate training data
      emitTestData(context, fd);//generate testing data
    }
  }

  /**
   * emit test data if one of airports in flight fd equals to resource or destination,
   * and year of flight is in given query year
   */
  private void emitTestData(Context context, FlightData fd)
      throws IOException, InterruptedException {
    for (String[] query : queryList) {

      String flightDate = getFlightDate(fd);
      String queryDate = query[0] + query[1] + query[2];
      String queryOrigin = query[3];
      String queryDes = query[4];
      String queryNextDate = getNextDate(queryDate);

      if (StringUtils.equals(queryDate, flightDate)) {
        //Emit Test LegOne(e.g. for route A->B->C, emit A->B)
        if (StringUtils.equals(queryOrigin, fd.getOrigin().toString())) {
          fd.setLegType(new IntWritable(1));
          RouteKey rk = new RouteKey(fd.getOrigin(), fd.getDest(), new Text(queryDes),
              new IntWritable(2), new Text(queryDate));
          context.write(rk, fd);
        }

        //Emit Test LegTwo(e.g. for route A->B->C, emit B->C)
        if (StringUtils.equals(queryDes, fd.getDest().toString())) {
          fd.setLegType(new IntWritable(2));
          RouteKey rk = new RouteKey(new Text(queryOrigin), fd.getOrigin(), fd.getDest(),
              new IntWritable(2), new Text(queryDate));
          context.write(rk, fd);
        }
      }

      if (StringUtils.equals(queryNextDate, flightDate)) {//if arrive at destination next day
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

  /**
   * return next date of the given date dt
   */
  private String getNextDate(String dt) {
    try {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
      Calendar c = Calendar.getInstance();
      c.setTime(sdf.parse(dt));
      c.add(Calendar.DATE, 1);  // number of days to add
      return sdf.format(c.getTime());
    } catch (ParseException pe) {
      return dt;
    }
  }

  /**
   * emit training data if one of airports in flight fd equals to resource or destination,
   * and year of flight is not in given query year
   */
  private void emitTrainData(Context context, FlightData fd)
      throws IOException, InterruptedException {
    //TODO: Change yearLength
    int trainingYearLen = 1;//select training data from 'yearLength' years
    if (fd.getYear().get() >= (computeYear - trainingYearLen) && fd.getYear().get() < computeYear) {
      writeLegOneTrainFlight(context, fd);//generate flight from source to intermediate hop
      writeLegTwoTrainFlight(context, fd);//generate flight from intermediate hop to destination
    }
  }

  /**
   * for a possible route A->B->C, write <key, value> key is [A, B, C, type(training or test), date];
   * value is FlightData of A->B
   */
  private void writeLegOneTrainFlight(Context context, FlightData fd)
      throws IOException, InterruptedException {
    fd.setLegType(new IntWritable(1));
    for (Map.Entry<String, Set<String>> entry : srcToDesMap.entrySet()) {
      if (StringUtils.equals(entry.getKey(), fd.getOrigin().toString())) {
        //if this flight is depart from the source(from query)
        for (String des : entry.getValue()) {
          RouteKey rk = new RouteKey(fd.getOrigin(), fd.getDest(), new Text(des),
              new IntWritable(1), new Text(getFlightDate(fd)));
          context.write(rk, fd);
        }
      }
    }

  }

  /**
   * for a possible route A->B->C, write <key, value>: key is [A, B, C, type(training or test), date];
   * value is FlightData of B->C
   */
  private void writeLegTwoTrainFlight(Context context, FlightData fd)
      throws IOException, InterruptedException {
    fd.setLegType(new IntWritable(2));
    for (Map.Entry<String, Set<String>> entry : desToSrcMap.entrySet()) {
      if (StringUtils.equals(entry.getKey(), fd.getDest().toString())) {
        //if this flight is arrive at the destination(from query)
        for (String origin : entry.getValue()) {
          RouteKey rk = new RouteKey(new Text(origin), fd.getOrigin(), fd.getDest(),
              new IntWritable(1), new Text(getFlightDate(fd)));
          context.write(rk, fd);
        }
      }
    }
  }
}