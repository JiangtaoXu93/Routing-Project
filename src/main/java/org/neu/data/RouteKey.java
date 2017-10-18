package org.neu.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class RouteKey implements WritableComparable<RouteKey> {

  private Text source;
  private Text hop;
  private Text destination;

  public RouteKey() {

    this.source = new Text();
    this.hop = new Text();
    this.destination = new Text();
  }

  public RouteKey(String source, String hop, String destination) {
    this(new Text(source), new Text(hop), new Text(destination));
  }

  public RouteKey(Text source, Text hop, Text destination) {
    this.source = source;
    this.hop = hop;
    this.destination = destination;
  }

  public Text getSource() {
    return source;
  }

  public void setSource(Text source) {
    this.source = source;
  }

  public Text getHop() {
    return hop;
  }

  public void setHop(Text hop) {
    this.hop = hop;
  }

  public Text getDestination() {
    return destination;
  }

  public void setDestination(Text destination) {
    this.destination = destination;
  }

  @Override
  public int compareTo(RouteKey o) {
    int val = this.source.compareTo(o.getSource());
    if (val == 0) {
      val = this.hop.compareTo(o.getHop());
      if (val == 0) {
        val = this.destination.compareTo(o.getDestination());
      }
    }
    return val;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    source.write(dataOutput);
    hop.write(dataOutput);
    destination.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    source.readFields(dataInput);
    hop.readFields(dataInput);
    destination.readFields(dataInput);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 31)
        .append(source.hashCode())
        .append(hop.hashCode())
        .append(destination.hashCode())
        .toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RouteKey) {
      RouteKey rk = (RouteKey) obj;
      return (rk == this) || (0 == compareTo(rk));
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(source.toString());
    sb.append(",");
    sb.append(hop.toString());
    sb.append(",");
    sb.append(destination.toString());
    return sb.toString();
  }
}
