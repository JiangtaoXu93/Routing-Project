package org.neu.util;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.neu.data.FlightData;

/**
 * @author Bhanu, Joyal, Jiangtao
 */
public class DataUtil {

  private static final short MAX_FIELDS = 110;
  public static Map<String, Integer> csvColumnMap = new HashMap<>();

  /*Initializes map containing CSV Column Mapping*/
  public static void initCsvColumnMap() {
    csvColumnMap.put("year", 0); //YEAR
    csvColumnMap.put("month", 2); //MONTH
    csvColumnMap.put("dayOfMonth", 3); //DAY_OF_MONTH
    csvColumnMap.put("dayOfWeek", 4); //DAY_OF_WEEK
    csvColumnMap.put("uniqueCarrier", 6);//UNIQUE_CARRIER
    csvColumnMap.put("airlineID", 7);//AIRLINE_ID
    csvColumnMap.put("flightId", 10);//FL_NUM
    csvColumnMap.put("airportID", 11);//ORIGIN_AIRPORT_ID
    csvColumnMap.put("airportSeqID", 12);//ORIGIN_AIRPORT_SEQ_ID
    csvColumnMap.put("origin", 14);//ORIGIN
    csvColumnMap.put("destAirportId", 20); //DEST_AIRPORT_ID
    csvColumnMap.put("cityMarketID", 22);//DEST_CITY_MARKET_ID
    csvColumnMap.put("destination", 23);//DEST
    csvColumnMap.put("cityName", 24);//DEST_CITY_NAME
    csvColumnMap.put("state", 25);//DEST_STATE_ABR
    csvColumnMap.put("stateFips", 26);//DEST_STATE_FIPS
    csvColumnMap.put("stateName", 27);//DEST_STATE_NM
    csvColumnMap.put("wac", 28);//DEST_WAC
    csvColumnMap.put("crsDepTime", 29);//CRS_DEP_TIME
    csvColumnMap.put("depTime", 30);//DEP_TIME
    csvColumnMap.put("depDelayNew", 32);//DEP_DELAY_NEW
    csvColumnMap.put("crsArrTime", 40);//CRS_ARR_TIME
    csvColumnMap.put("arrTime", 41);//ARR_TIME
    csvColumnMap.put("arrDelay", 42);//ARR_Delay
    csvColumnMap.put("arrDelayMinutes", 43);//ARR_DELAY_NEW
    csvColumnMap.put("arrDel15", 44);//ARR_DEL15
    csvColumnMap.put("cancelled", 47);//CANCELLED
    csvColumnMap.put("crsElapsedTime", 50);//CRS_ELAPSED_TIME
    csvColumnMap.put("actualElapsedTime", 51);//ACTUAL_ELAPSED_TIME
    //csvColumnMap.put("weatherDelay",57); //WEATHER_DELAY
  }

  public static FlightData getFlightData(Text value) throws NumberFormatException {
    CSVRecord d = new CSVRecord(value.toString());
    FlightData fd = null;
    try {
      if (MAX_FIELDS == d.fieldCount &&
          ifFieldsAreNotEmpty(d) &&
          validateTimes(d) && isPositive(d)) {
        fd = new FlightData();
        fd.setYear(new IntWritable(Integer.parseInt(d.get(csvColumnMap.get("year")))));
        fd.setMonth(new IntWritable(Integer.parseInt(d.get(csvColumnMap.get("month")))));
        fd.setDayOfWeek(new IntWritable(Integer.parseInt(d.get(csvColumnMap.get("dayOfWeek")))));
        fd.setDayOfMonth(new IntWritable(Integer.parseInt(d.get(csvColumnMap.get("dayOfMonth")))));
        fd.setHourOfDay(new IntWritable(getHourOfDay(d.get(csvColumnMap.get("crsDepTime")))));
        fd.setFlightId(new IntWritable(Integer.parseInt(d.get(csvColumnMap.get("flightId")))));
        fd.setCarrier(new Text(d.get(csvColumnMap.get("uniqueCarrier"))));
        fd.setOrigin(new Text(d.get(csvColumnMap.get("origin"))));
        fd.setDest(new Text(d.get(csvColumnMap.get("destination"))));
        fd.setSchDepTime(new Text(d.get(csvColumnMap.get("crsDepTime"))));
        fd.setActDepTime(new Text(d.get(csvColumnMap.get("depTime"))));
        fd.setSchArrTime(new Text(d.get(csvColumnMap.get("crsArrTime"))));
        fd.setActArrTime(new Text(d.get(csvColumnMap.get("arrTime"))));
        fd.setArrDelay(getDelayMinutes(d, "arrDelayMinutes"));
        fd.setDepDelay(getDelayMinutes(d, "depDelayNew"));
        fd.setSchElapsedTime(new Text(d.get(csvColumnMap.get("crsElapsedTime"))));
        fd.setActElapsedTime(new Text(d.get(csvColumnMap.get("actualElapsedTime"))));
        fd.setCancelled(
            new BooleanWritable(BooleanUtils.toBoolean(d.get(csvColumnMap.get("cancelled")))));
      }
    } catch (NumberFormatException nfe) {
      // Do Nothing
    }
    return fd;
  }

  private static int getHourOfDay(String crsDepTime) {
    crsDepTime = StringUtils.leftPad(crsDepTime, 4, '0');
    return Integer.parseInt(crsDepTime.substring(0, 2));
  }

  private static FloatWritable getDelayMinutes(CSVRecord d, String actMinutes) {
    Float delay;
    if (Integer.parseInt(d.get(csvColumnMap.get("cancelled"))) == 1) {
      delay = 4F;
    } else {
      delay = Float.parseFloat(d.get(csvColumnMap.get(actMinutes))) /
          Float.parseFloat(d.get(csvColumnMap.get("crsElapsedTime")));
    }
    return new FloatWritable(delay);
  }

  /**
   * @return true if all above parameters are greater than 0, false otherwise
   */
  private static boolean isPositive(CSVRecord d) {
    return Integer.parseInt(d.get(csvColumnMap.get("airportID"))) > 0
        && Integer.parseInt(d.get(csvColumnMap.get("airportSeqID"))) > 0
        && Integer.parseInt(d.get(csvColumnMap.get("cityMarketID"))) > 0
        && Integer.parseInt(d.get(csvColumnMap.get("stateFips"))) > 0
        && Integer.parseInt(d.get(csvColumnMap.get("wac"))) > 0;
  }

  /**
   * @return Calculates timezone and checks if timezone mod 60 is zero. If it is zero returns true,
   * false otherwise.
   */
  private static boolean validateTimes(CSVRecord d) {
    int timeZone;
    int crsArrTime;
    int crsDepTime;
    int crsElapsedTime;
    int arrDelay;
    int arrDelayMinutes;

    try {

      crsArrTime = Integer.parseInt(d.get(csvColumnMap.get("crsArrTime")));
      crsDepTime = Integer.parseInt(d.get(csvColumnMap.get("crsDepTime")));
      crsElapsedTime = Integer.parseInt(d.get(csvColumnMap.get("crsElapsedTime")));
      arrDelay = (int) Float.parseFloat(d.get(csvColumnMap.get("arrDelay")));
      arrDelayMinutes = (int) Float.parseFloat(d.get(csvColumnMap.get("arrDelayMinutes")));

      // CRSArrTime and CRSDepTime and CRSElapsedTime should not be zero
      if (crsArrTime == 0 || crsDepTime == 0 || crsElapsedTime == 0) {
        return false;
      }

      //timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime;
      timeZone = crsArrTime - crsDepTime - crsElapsedTime;

      //timeZone % 60 should be 0
      if ((timeZone % 60) != 0) {
        return false;
      }

      // For flights that are not Cancelled:
      if (Integer.parseInt(d.get(csvColumnMap.get("cancelled"))) != 1) {
        //ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
        if (Integer.parseInt(d.get(csvColumnMap.get("arrTime"))) - Integer
            .parseInt(d.get(csvColumnMap.get("depTime"))) - Integer
            .parseInt(d.get(csvColumnMap.get("actualElapsedTime"))) - timeZone != 0) {
          return false;
        }

        //if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes
        //if ArrDelay < 0 then ArrDelayMinutes should be zero
        if (arrDelay > 0 && arrDelay != arrDelayMinutes) {
          return false;
        } else if (arrDelay < 0 && arrDelayMinutes != 0) {
          return false;
        }
        //if ArrDelayMinutes >= 15 then ArrDel15 should be true
        if (arrDelayMinutes >= 15) {
          return Boolean.parseBoolean(d.get(csvColumnMap.get("arrDel15")));
        }
      }


    } catch (NumberFormatException nfe) {
      return false;
    }
    return true;

  }
  
  public static String addLeftPad(String s) {
	  return StringUtils.leftPad(s, 2, '0');//add 0 before s if length of s is less than 2;  
  }

  /**
   * @return False if any of the field is empty
   */
  private static boolean ifFieldsAreNotEmpty(CSVRecord d) {
    for (int i : csvColumnMap.values()) {
      if (StringUtils.isEmpty(d.get(i))) {
        return false;
      }
    }
    return true;
  }

}
