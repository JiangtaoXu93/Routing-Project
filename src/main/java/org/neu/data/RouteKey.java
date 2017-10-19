package org.neu.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class RouteKey implements WritableComparable<RouteKey> {

  private Text source;
  private Text hop;
  private Text destination;
  private IntWritable type;//1-Train;2-Test
  private Text date;//yyyyMMdd

  public RouteKey() {

    this.source = new Text();
    this.hop = new Text();
    this.destination = new Text();
    this.type = new IntWritable();
    this.date = new Text();
  }

  public RouteKey(String source, String hop, String destination,
      Integer type, String date) {
    this(new Text(source), new Text(hop), new Text(destination), new IntWritable(type),
        new Text(date));
  }

  public RouteKey(Text source, Text hop, Text destination, IntWritable type,
      Text date) {
    this.source = source;
    this.hop = hop;
    this.destination = destination;
    this.type = type;
    this.date = date;
  }

  public IntWritable getType() {
    return type;
  }

  public void setType(IntWritable type) {
    this.type = type;
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
    int val = this.type.compareTo(o.getType());
    if (val == 0) {
      val = this.source.compareTo(o.getSource());
      if (val == 0) {
        val = this.hop.compareTo(o.getHop());
        if (val == 0) {
          val = this.destination.compareTo(o.getDestination());
          if (val == 0) {
            val = this.date.compareTo(o.getDate());
          }
        }
      }
    }
    return val;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    source.write(dataOutput);
    hop.write(dataOutput);
    destination.write(dataOutput);
    type.write(dataOutput);
    date.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    source.readFields(dataInput);
    hop.readFields(dataInput);
    destination.readFields(dataInput);
    type.readFields(dataInput);
    date.readFields(dataInput);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 31)
        .append(type.hashCode())
        .append(source.hashCode())
        .append(hop.hashCode())
        .append(destination.hashCode())
        .append(date.hashCode())
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
    sb.append(type);
    sb.append(",");
    sb.append(source);
    sb.append(",");
    sb.append(hop);
    sb.append(",");
    sb.append(destination);
    sb.append(",");
    sb.append(date);
    return sb.toString();
  }

  public Text getDate() {
    return date;
  }

  public void setDate(Text date) {
    this.date = date;
  }
}
