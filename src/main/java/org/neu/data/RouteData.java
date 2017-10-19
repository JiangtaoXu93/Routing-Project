package org.neu.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class RouteData implements Writable {

  private FlightData legOne;
  private FlightData legTwo;
  private IntWritable isValid; //0-No Label,1-Not Valid,2-Valid

  public RouteData() {
    this.legOne = new FlightData();
    this.legTwo = new FlightData();
    this.isValid = new IntWritable();
  }

  public RouteData(FlightData legOne, FlightData legTwo, IntWritable isValid) {
    this.legOne = legOne;
    this.legTwo = legTwo;
    this.isValid = isValid;
  }

  @Override
  public String toString() {
    return legOne + "," + legTwo + "," + isValid;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    legOne.write(dataOutput);
    legTwo.write(dataOutput);
    isValid.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    legOne.readFields(dataInput);
    legTwo.readFields(dataInput);
    isValid.readFields(dataInput);
  }
}
