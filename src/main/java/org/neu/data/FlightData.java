package org.neu.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
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
  private FloatWritable arrDelay;//ARR_DELAY_NEW (mm)
  private FloatWritable depDelay;//DEP_DELAY_NEW (mm)
  private Text schElapsedTime;//CRS_ELAPSED_TIME (hhmm)
  private Text actElapsedTime;//ELAPSED_TIME (hhmm)

  public FlightData(IntWritable legType, IntWritable year, IntWritable month,
      IntWritable dayOfWeek, IntWritable dayOfMonth, IntWritable hourOfDay,
      IntWritable flightId, Text carrier, Text origin, Text dest,
      Text schDepTime, Text actDepTime, Text schArrTime, Text actArrTime,
      FloatWritable arrDelay, FloatWritable depDelay, Text schElapsedTime, Text actElapsedTime) {
    this.legType = legType;
    this.year = year;
    this.month = month;
    this.dayOfWeek = dayOfWeek;
    this.dayOfMonth = dayOfMonth;
    this.hourOfDay = hourOfDay;
    this.flightId = flightId;
    this.carrier = carrier;
    this.origin = origin;
    this.dest = dest;
    this.schDepTime = schDepTime;
    this.actDepTime = actDepTime;
    this.schArrTime = schArrTime;
    this.actArrTime = actArrTime;
    this.arrDelay = arrDelay;
    this.depDelay = depDelay;
    this.schElapsedTime = schElapsedTime;
    this.actElapsedTime = actElapsedTime;
  }

  public FlightData(int legType, int year, int month,
      int dayOfWeek, int dayOfMonth, int hourOfDay,
      int flightId, String carrier, String origin, String dest, String schDepTime,
      String actDepTime, String schArrTime, String actArrTime,
      Float arrDelay, Float depDelay, String schElapsedTime, String actElapsedTime) {
    this(new IntWritable(legType),
        new IntWritable(year),
        new IntWritable(month),
        new IntWritable(dayOfWeek),
        new IntWritable(dayOfMonth),
        new IntWritable(hourOfDay),
        new IntWritable(flightId),
        new Text(carrier),
        new Text(origin),
        new Text(dest),
        new Text(schDepTime),
        new Text(actArrTime),
        new Text(actDepTime),
        new Text(schArrTime),
        new FloatWritable(arrDelay),
        new FloatWritable(depDelay),
        new Text(schElapsedTime),
        new Text(actElapsedTime));
  }


  public FlightData() {
  /*  this(new IntWritable(), new IntWritable(), new IntWritable(), new IntWritable(),
        new IntWritable(), new IntWritable(), new IntWritable(), new Text(), new Text(), new Text(),
        new Text(), new Text(), new Text(), new Text(), new Text(), new Text(), new Text(),
        new Text());*/
  }

  public IntWritable getLegType() {
    return legType;
  }

  public void setLegType(IntWritable legType) {
    this.legType = legType;
  }

  public IntWritable getYear() {
    return year;
  }

  public void setYear(IntWritable year) {
    this.year = year;
  }

  public IntWritable getMonth() {
    return month;
  }

  public void setMonth(IntWritable month) {
    this.month = month;
  }

  public IntWritable getDayOfWeek() {
    return dayOfWeek;
  }

  public void setDayOfWeek(IntWritable dayOfWeek) {
    this.dayOfWeek = dayOfWeek;
  }

  public IntWritable getDayOfMonth() {
    return dayOfMonth;
  }

  public void setDayOfMonth(IntWritable dayOfMonth) {
    this.dayOfMonth = dayOfMonth;
  }

  public IntWritable getHourOfDay() {
    return hourOfDay;
  }

  public void setHourOfDay(IntWritable hourOfDay) {
    this.hourOfDay = hourOfDay;
  }

  public IntWritable getFlightId() {
    return flightId;
  }

  public void setFlightId(IntWritable flightId) {
    this.flightId = flightId;
  }

  public Text getCarrier() {
    return carrier;
  }

  public void setCarrier(Text carrier) {
    this.carrier = carrier;
  }

  public Text getOrigin() {
    return origin;
  }

  public void setOrigin(Text origin) {
    this.origin = origin;
  }

  public Text getDest() {
    return dest;
  }

  public void setDest(Text dest) {
    this.dest = dest;
  }

  public Text getSchDepTime() {
    return schDepTime;
  }

  public void setSchDepTime(Text schDepTime) {
    this.schDepTime = schDepTime;
  }

  public Text getActDepTime() {
    return actDepTime;
  }

  public void setActDepTime(Text actDepTime) {
    this.actDepTime = actDepTime;
  }

  public Text getSchArrTime() {
    return schArrTime;
  }

  public void setSchArrTime(Text schArrTime) {
    this.schArrTime = schArrTime;
  }

  public Text getActArrTime() {
    return actArrTime;
  }

  public void setActArrTime(Text actArrTime) {
    this.actArrTime = actArrTime;
  }

  public FloatWritable getArrDelay() {
    return arrDelay;
  }

  public void setArrDelay(FloatWritable arrDelay) {
    this.arrDelay = arrDelay;
  }

  public FloatWritable getDepDelay() {
    return depDelay;
  }

  public void setDepDelay(FloatWritable depDelay) {
    this.depDelay = depDelay;
  }

  public Text getSchElapsedTime() {
    return schElapsedTime;
  }

  public void setSchElapsedTime(Text schElapsedTime) {
    this.schElapsedTime = schElapsedTime;
  }

  public Text getActElapsedTime() {
    return actElapsedTime;
  }

  public void setActElapsedTime(Text actElapsedTime) {
    this.actElapsedTime = actElapsedTime;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {

  }
}
