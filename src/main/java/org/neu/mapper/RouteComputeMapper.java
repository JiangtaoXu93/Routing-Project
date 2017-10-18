package org.neu.mapper;

import static org.neu.util.InputDataUtil.csvColumnMap;
import static org.neu.util.InputDataUtil.isValidRecord;

import com.opencsv.CSVParser;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.neu.data.FlightData;
import org.neu.data.RouteKey;


public class RouteComputeMapper extends
    Mapper<LongWritable, Text, RouteKey, FlightData> {

  private static Set<String> sourceSet = new HashSet<>();
  private static Set<String> destinationSet = new HashSet<>();
  private static int year;
  private static CSVParser csvParser = new CSVParser();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {

  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] flightRecord = csvParser.parseLine(value.toString());

    if (flightRecord.length > 0 && flightRecord.length <= 110 && isValidRecord(flightRecord)) {
      Float delayMinutes = getDelayMinutes(flightRecord);

    }

  }

  private Float getDelayMinutes(String[] flightRecord) {
    Float delay;
    if (Integer.parseInt(flightRecord[csvColumnMap.get("cancelled")]) == 1) {
      delay = 4F;
    } else {
      delay = Float.parseFloat(flightRecord[csvColumnMap.get("arrDelayMinutes")]) /
          Float.parseFloat(flightRecord[csvColumnMap.get("crsElapsedTime")]);
    }
    return delay;
  }
}