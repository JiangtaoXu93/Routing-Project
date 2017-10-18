package org.neu.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class FlightData implements Writable {

  /**
   * LegType is the flight type in one complete route.
   * If the route is A->C->B
   * LegType=1 -> Flight A-C
   * LegType=2 -> Flight C-B
   */
  private IntWritable legType;
  private IntWritable year;//YEAR
  private IntWritable month;//MONTH
  private IntWritable dayOfWeek;//DAY_OFF_WEEK
  private IntWritable dayOfMonth;//DAY_OF_MONTH
  private IntWritable hourOfDay;//Computed from
  private IntWritable flightId;//FL_NUM
  private Text carrier;//UNIQUE_CARRIER
  private Text origin;//ORIGIN
  private Text dest;//DEST
  private Text schDepTime;//CRS_DEP_TIME (local time: hhmm)
  private Text actDepTime;//DEP_TIME (local time: hhmm)
  private Text schArrTime;//CRS_ARR_TIME (local time: hhmm)
  private Text actArrTime;//ARR_TIME (local time: hhmm)
  private Text arrDelay;//ARR_DELAY_NEW (mm)
  private Text depDelay;//DEP_DELAY_NEW (mm)
  private Text schElapsedTime;//CRS_ELAPSED_TIME (hhmm)
  private Text actElapsedTime;//ELAPSED_TIME (hhmm)


  @Override
  public void write(DataOutput dataOutput) throws IOException {

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {

  }
}
