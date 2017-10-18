package org.neu.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class FlightData implements Writable {

  public static final String SEP_COMMA = ",";
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
  private FloatWritable arrDelay;//NORMALISED_DELAY
  private FloatWritable depDelay;//NORMALISED_DELAY
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
        new Text(actDepTime),
        new Text(schArrTime),
        new Text(actArrTime),
        new FloatWritable(arrDelay),
        new FloatWritable(depDelay),
        new Text(schElapsedTime),
        new Text(actElapsedTime));
  }

  public FlightData(FlightData fd) {
    this(new IntWritable(fd.getLegType().get()),
        new IntWritable(fd.getYear().get()),
        new IntWritable(fd.getMonth().get()),
        new IntWritable(fd.getDayOfWeek().get()),
        new IntWritable(fd.getDayOfMonth().get()),
        new IntWritable(fd.getHourOfDay().get()),
        new IntWritable(fd.getFlightId().get()),
        new Text(fd.getCarrier().toString()),
        new Text(fd.getOrigin().toString()),
        new Text(fd.getDest().toString()),
        new Text(fd.getSchDepTime().toString()),
        new Text(fd.getActDepTime().toString()),
        new Text(fd.getSchArrTime().toString()),
        new Text(fd.getActArrTime().toString()),
        new FloatWritable(fd.getArrDelay().get()),
        new FloatWritable(fd.getDepDelay().get()),
        new Text(fd.getSchElapsedTime().toString()),
        new Text(fd.getActElapsedTime().toString()));
  }

  public FlightData() {
    this(new IntWritable(), new IntWritable(), new IntWritable(), new IntWritable(),
        new IntWritable(), new IntWritable(), new IntWritable(), new Text(),
        new Text(), new Text(), new Text(), new Text(), new Text(), new Text(),
        new FloatWritable(), new FloatWritable(), new Text(), new Text());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(legType);
    sb.append(SEP_COMMA);
    sb.append(year);
    sb.append(SEP_COMMA);
    sb.append(month);
    sb.append(SEP_COMMA);
    sb.append(dayOfWeek);
    sb.append(SEP_COMMA);
    sb.append(dayOfMonth);
    sb.append(SEP_COMMA);
    sb.append(hourOfDay);
    sb.append(SEP_COMMA);
    sb.append(flightId);
    sb.append(SEP_COMMA);
    sb.append(carrier);
    sb.append(SEP_COMMA);
    sb.append(origin);
    sb.append(SEP_COMMA);
    sb.append(dest);
    sb.append(SEP_COMMA);
    sb.append(schDepTime);
    sb.append(SEP_COMMA);
    sb.append(actDepTime);
    sb.append(SEP_COMMA);
    sb.append(schArrTime);
    sb.append(SEP_COMMA);
    sb.append(actArrTime);
    sb.append(SEP_COMMA);
    sb.append(arrDelay);
    sb.append(SEP_COMMA);
    sb.append(depDelay);
    sb.append(SEP_COMMA);
    sb.append(schElapsedTime);
    sb.append(SEP_COMMA);
    sb.append(actElapsedTime);
    return sb.toString();
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
    legType.write(dataOutput);
    year.write(dataOutput);
    month.write(dataOutput);
    dayOfWeek.write(dataOutput);
    dayOfMonth.write(dataOutput);
    hourOfDay.write(dataOutput);
    flightId.write(dataOutput);
    carrier.write(dataOutput);
    origin.write(dataOutput);
    dest.write(dataOutput);
    schDepTime.write(dataOutput);
    actDepTime.write(dataOutput);
    schArrTime.write(dataOutput);
    actArrTime.write(dataOutput);
    arrDelay.write(dataOutput);
    depDelay.write(dataOutput);
    schElapsedTime.write(dataOutput);
    actElapsedTime.write(dataOutput);

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    legType.readFields(dataInput);
    year.readFields(dataInput);
    month.readFields(dataInput);
    dayOfWeek.readFields(dataInput);
    dayOfMonth.readFields(dataInput);
    hourOfDay.readFields(dataInput);
    flightId.readFields(dataInput);
    carrier.readFields(dataInput);
    origin.readFields(dataInput);
    dest.readFields(dataInput);
    schDepTime.readFields(dataInput);
    actDepTime.readFields(dataInput);
    schArrTime.readFields(dataInput);
    actArrTime.readFields(dataInput);
    arrDelay.readFields(dataInput);
    depDelay.readFields(dataInput);
    schElapsedTime.readFields(dataInput);
    actElapsedTime.readFields(dataInput);
  }
}
