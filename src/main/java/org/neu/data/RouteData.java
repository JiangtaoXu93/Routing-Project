package org.neu.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class RouteData implements Writable {

  private FlightData legOne;
  private FlightData legTwo;

  public RouteData() {
    this.legOne = new FlightData();
    this.legTwo = new FlightData();
  }

  public RouteData(FlightData legOne, FlightData legTwo) {
    this.legOne = legOne;
    this.legTwo = legTwo;
  }

  @Override
  public String toString() {
    return legOne + "," + legTwo;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    legOne.write(dataOutput);
    legTwo.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    legOne.readFields(dataInput);
    legTwo.readFields(dataInput);
  }
}
