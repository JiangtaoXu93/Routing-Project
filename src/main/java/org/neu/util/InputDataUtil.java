package org.neu.util;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

/**
 * @author Bhanu, Joyal, Jiangtao
 */
public class InputDataUtil {

  private static final int FLIGHT_DELAY_FACTOR = 4;
  public static Map<String, Integer> csvColumnMap = new HashMap<>();

  /*Initializes map containing CSV Column Mapping*/
  public static void initCsvColumnMap() {
    csvColumnMap.put("year", 0); //YEAR
    csvColumnMap.put("month", 2); //MONTH
    csvColumnMap.put("airlineID", 7);//AIRLINE_ID
    csvColumnMap.put("uniqueCarrier", 6);//UNIQUE_CARRIER
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
    csvColumnMap.put("crsArrTime", 40);//CRS_ARR_TIME
    csvColumnMap.put("arrTime", 41);//ARR_TIME
    csvColumnMap.put("arrDelay", 42);//ARR_Delay
    csvColumnMap.put("arrDelayMinutes", 43);//ARR_DELAY_NEW
    csvColumnMap.put("arrDel15", 44);//ARR_DEL15
    csvColumnMap.put("cancelled", 47);//CANCELLED
    csvColumnMap.put("crsElapsedTime", 50);//CRS_ELAPSED_TIME
    csvColumnMap.put("actualElapsedTime", 51);//ActualElapsedTime
  }

  /**
   * @return True if the record is valid.
   */
  public static boolean isValidRecord(String[] record) {
    return ifFieldsAreNotEmpty(record)
        && validateTimes(record)
        && isPositive(record);
  }

  /**
   * @return False if any of the field is empty
   */
  private static boolean ifFieldsAreNotEmpty(String[] record) {
    for (int i : csvColumnMap.values()) {
      if (StringUtils.isEmpty(record[i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return Calculates timezone and checks if timezone mod 60 is zero. If it is zero returns true,
   * false otherwise.
   */
  private static boolean validateTimes(String[] record) {
    int timeZone;
    int crsArrTime;
    int crsDepTime;
    int crsElapsedTime;
    int arrDelay;
    int arrDelayMinutes;

    try {

      crsArrTime = Integer.parseInt(record[csvColumnMap.get("crsArrTime")]);
      crsDepTime = Integer.parseInt(record[csvColumnMap.get("crsDepTime")]);
      crsElapsedTime = Integer.parseInt(record[csvColumnMap.get("crsElapsedTime")]);
      arrDelay = (int) Float.parseFloat(record[csvColumnMap.get("arrDelay")]);
      arrDelayMinutes = (int) Float.parseFloat(record[csvColumnMap.get("arrDelayMinutes")]);

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
      if (Integer.parseInt(record[csvColumnMap.get("cancelled")]) != 1) {
        //ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
        if (Integer.parseInt(record[csvColumnMap.get("arrTime")]) - Integer
            .parseInt(record[csvColumnMap.get("depTime")]) - Integer
            .parseInt(record[csvColumnMap.get("actualElapsedTime")]) - timeZone != 0) {
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
          return Boolean.parseBoolean(record[csvColumnMap.get("arrDel15")]);
        }
      }


    } catch (NumberFormatException nfe) {
      return false;
    }
    return true;
  }

  /**
   * @return true if all above parameters are greater than 0, false otherwise
   */
  private static boolean isPositive(String[] record) {
    return Integer.parseInt(record[csvColumnMap.get("airportID")]) > 0
        && Integer.parseInt(record[csvColumnMap.get("airportSeqID")]) > 0
        && Integer.parseInt(record[csvColumnMap.get("cityMarketID")]) > 0
        && Integer.parseInt(record[csvColumnMap.get("stateFips")]) > 0
        && Integer.parseInt(record[csvColumnMap.get("wac")]) > 0;
  }

}
