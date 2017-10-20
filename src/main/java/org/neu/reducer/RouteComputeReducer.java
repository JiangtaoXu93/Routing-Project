package org.neu.reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.neu.data.FlightData;
import org.neu.data.RouteData;
import org.neu.data.RouteKey;

public class RouteComputeReducer extends Reducer<RouteKey, FlightData, RouteKey, RouteData> {

  private static SimpleDateFormat hopTimeFormatter = new SimpleDateFormat("yyyyMMddHHmm");
  private MultipleOutputs mos;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    mos = new MultipleOutputs<>(context);
  }

  @Override
  protected void reduce(RouteKey key, Iterable<FlightData> values, Context context)
      throws IOException, InterruptedException {
    List<FlightData> legOneFlights = new ArrayList<>();
    List<FlightData> legTwoFlights = new ArrayList<>();
    partitionFlights(values, legOneFlights, legTwoFlights);
    computeAndEmitRoutes(key, context, legOneFlights, legTwoFlights);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    mos.close();
  }

  private void computeAndEmitRoutes(RouteKey key, Context context, List<FlightData> legOneFlights,
      List<FlightData> legTwoFlights) throws IOException, InterruptedException {
    for (FlightData lOne : legOneFlights) {
      for (FlightData lTwo : legTwoFlights) {
        String schLegOneArrTime = getDateTime(lOne.getYear().toString(),
            lOne.getMonth().toString(),
            lOne.getDayOfMonth().toString(),
            lOne.getSchArrTime().toString());

        String schLegTwoDeptTime = getDateTime(lTwo.getYear().toString(),
            lTwo.getMonth().toString(),
            lTwo.getDayOfMonth().toString(),
            lTwo.getSchDepTime().toString());

        if (checkValidConnection(schLegOneArrTime, schLegTwoDeptTime)) {
          writeRoutes(key, context, lOne, lTwo);
        }
      }
    }
  }

  private void partitionFlights(Iterable<FlightData> values, List<FlightData> legOneFlights,
      List<FlightData> legTwoFlights) {
    for (FlightData fd : values) {
      if (1 == fd.getLegType().get()) {
        legOneFlights.add(new FlightData(fd));
      } else {
        legTwoFlights.add(new FlightData(fd));
      }
    }
  }

  private void writeRoutes(RouteKey key, Context context, FlightData lOne, FlightData lTwo)
      throws IOException, InterruptedException {
    //Add Label to Train Route
    if (1 == key.getType().get()) {
      mos.write("train", key,
          new RouteData(lOne, lTwo, new IntWritable(getRouteLabel(lOne, lTwo))));
    } else {
      mos.write("test", key, new RouteData(lOne, lTwo, new IntWritable()));
      mos.write("validate", key,
          new RouteData(lOne, lTwo, new IntWritable(getRouteLabel(lOne, lTwo))));
    }
  }

  private String getDateTime(String year, String month, String dayOfMonth, String time) {
    return year + StringUtils.leftPad(month, 2, '0')
        + StringUtils.leftPad(dayOfMonth, 2, '0') + time;
  }

  private boolean checkValidConnection(String legOneArrTime, String legTwoDepTime) {
    Date hopArr;
    Date hopDep;
    try {
      hopArr = hopTimeFormatter.parse(legOneArrTime);
      hopDep = hopTimeFormatter.parse(legTwoDepTime);
      long diffMinutes = (hopDep.getTime() - hopArr.getTime()) / (60 * 1000) % 60;
      return diffMinutes <= 720 && diffMinutes >= 45;
    } catch (ParseException e) {
      e.printStackTrace();//TODO: Remove
      return false;
    }
  }

  private int getRouteLabel(FlightData lOne, FlightData lTwo) {

    if (lOne.getCancelled().get() || lTwo.getCancelled().get()) {
      return 1;
    }
    String actLegOneArrTime = getDateTime(lOne.getYear().toString(),
        lOne.getMonth().toString(),
        lOne.getDayOfMonth().toString(),
        lOne.getActArrTime().toString());

    String actLegTwoDeptTime = getDateTime(lTwo.getYear().toString(),
        lTwo.getMonth().toString(),
        lTwo.getDayOfMonth().toString(),
        lTwo.getActDepTime().toString());
    return checkValidConnection(actLegOneArrTime, actLegTwoDeptTime) ? 2 : 1;
  }
}