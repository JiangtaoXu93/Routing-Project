package org.neu.util;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.neu.data.BenchmarkData;

/**
 * @author Bhanu, Joyal, Jiangtao
 */
public class BenchmarkUtil {

  public static BenchmarkData getBenchmarkData(int iteration, int threadCount, String input,
      String k) {
    BenchmarkData tb = new BenchmarkData();
    tb.setThreadCount(threadCount);
    tb.setIteration(iteration);
    tb.setDateTime(new Date());
    tb.setInput(input);
    tb.setQueryFile(k);
    return tb;
  }


  public static void writeBenchmarks(List<BenchmarkData> benchmarkData, String location)
      throws IOException, URISyntaxException {

    OutputStream outputStream = getOutStream(location, new Configuration(), false);

    StringBuilder row = new StringBuilder();
    writeHeader(row, new ArrayList<String>() {{
      add("ExecutionDate");
      add("Input");
      add("ThreadCount");
      add("k");
      add("Iteration");
      add("TotalExecutionTime");
      add("RouteComputeJobTime");
    }});
    for (BenchmarkData bmd : benchmarkData) {
      writeRow(row, bmd);
    }
    outputStream.write(row.toString().getBytes());
    outputStream.flush();
    outputStream.close();
  }

  private static OutputStream getOutStream(String location, Configuration conf, boolean append)
      throws URISyntaxException, IOException {
    URI locUri = new URI(location);
    FileSystem fs = FileSystem.get(locUri, conf);
    if (append) {
      return fs.append(new Path(locUri));
    } else {
      return fs.create(new Path(locUri), true);
    }
  }

  private static void writeRow(StringBuilder row, BenchmarkData bmd) {
    row.append(bmd.getDateTime());
    row.append(",");
    row.append(bmd.getInput());
    row.append(",");
    row.append(bmd.getThreadCount());
    row.append(",");
    row.append(bmd.getQueryFile());
    row.append(",");
    row.append(bmd.getIteration());
    row.append(",");
    row.append(bmd.getTotalExecutionTime());
    row.append(",");
    row.append(bmd.getRouteComputeJob());
    row.append(",");
    row.append("\n");
  }

  private static void writeHeader(StringBuilder row, List<String> headers) {
    for (int i = 0; i < headers.size(); i++) {
      row.append(headers.get(i));
      if (i < headers.size() - 1) {
        row.append(",");
      }
    }
    row.append("\n");
  }

  public static void writeBenchmark(BenchmarkData bmd, String location, Configuration conf)
      throws URISyntaxException, IOException {
    OutputStream outputStream = getOutStream(location, conf, true);
    StringBuilder row = new StringBuilder();
    writeRow(row, bmd);
    outputStream.write(row.toString().getBytes());
    outputStream.flush();
    outputStream.close();

  }
}
