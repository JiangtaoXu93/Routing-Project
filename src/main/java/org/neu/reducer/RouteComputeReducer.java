package org.neu.reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.neu.data.FlightData;
import org.neu.data.RouteData;
import org.neu.data.RouteKey;

public class RouteComputeReducer extends Reducer<RouteKey, FlightData, RouteKey, RouteData> {

  private static SimpleDateFormat hopTimeFormatter = new SimpleDateFormat("yyyyMMddHHmm");


  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
  }

  @Override
  protected void reduce(RouteKey key, Iterable<FlightData> values, Context context)
      throws IOException, InterruptedException {
    System.out.println(key);
    List<FlightData> legOneFlights = new ArrayList<>();
    List<FlightData> legTwoFlights = new ArrayList<>();
    for (FlightData fd : values) {
      if (1 == fd.getLegType().get()) {
        legOneFlights.add(new FlightData(fd));
      } else {
        legTwoFlights.add(new FlightData(fd));
      }
    }

    for (FlightData lOne : legOneFlights) {
      for (FlightData lTwo : legTwoFlights) {
        Date hopArr;
        Date hopDep;
        try {
          hopArr = hopTimeFormatter.parse(
              lOne.getYear().toString()
                  + StringUtils.leftPad(lOne.getMonth().toString(), 2, '0')
                  + StringUtils.leftPad(lOne.getDayOfMonth().toString(), 2, '0')
                  + lOne.getSchArrTime().toString());
          hopDep = hopTimeFormatter.parse(
              lTwo.getYear().toString()
                  + StringUtils.leftPad(lTwo.getMonth().toString(), 2, '0')
                  + StringUtils.leftPad(lTwo.getDayOfMonth().toString(), 2, '0')
                  + lTwo.getSchDepTime().toString());
          long diffMinutes = (hopDep.getTime() - hopArr.getTime()) / (60 * 1000) % 60;
          System.out.println(diffMinutes);
          if (diffMinutes <= 1440 && diffMinutes >= 45) {
            context.write(key, new RouteData(lOne, lTwo));
          }
        } catch (ParseException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
