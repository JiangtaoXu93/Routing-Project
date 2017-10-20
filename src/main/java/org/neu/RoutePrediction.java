package org.neu;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.neu.data.BenchmarkData;
import org.neu.job.RouteComputeJob;
import org.neu.util.BenchmarkUtil;

/**
 * RoutePrediction: MapReduce driver, and run benchmark
 * @author Bhanu, Joyal, Jiangtao
 */
public class RoutePrediction {

	public static void main(String[] args) throws Exception {
		if (args.length < 5) {
			System.out.println("Invalid Arguments.");
			System.exit(1);
		}
		String benchmarkLoc = String
				.format("%s/benchmark-fd-%s.csv", args[4], System.currentTimeMillis());
		List<BenchmarkData> bmdList = new ArrayList<>();

		int iterations = Integer.valueOf(args[0]);
		for (int i = 1; i <= iterations; i++) {
			long d1 = System.nanoTime();
			BenchmarkData bmd = BenchmarkUtil.getBenchmarkData(i, 0, args[2], args[1]);
			Configuration conf = new Configuration();
			runJobs(args, conf, bmd);
			long d2 = System.nanoTime();
			final double totalTime = (d2 - d1) / 1000000;
			bmd.setTotalExecutionTime(totalTime);
			bmdList.add(bmd);
		}
		BenchmarkUtil.writeBenchmarks(bmdList, benchmarkLoc);
	}


	/**
	 * Driver method to run the RouteComputeJob
	 */
	private static void runJobs(String[] args, Configuration conf, BenchmarkData bmd)
			throws Exception {
		int result;
		long d1;
		long d2;
		cleanOutDir(args[3], conf);

		//RouteComputeJob
		d1 = System.nanoTime();
		System.out.println(String
				.format(">>>>>> Running RouteComputeJob [Iteration=%s, Query=%s]", bmd.getIteration(),
						bmd.getQueryFile()));
		result = ToolRunner.run(conf, new RouteComputeJob(), args);
		if (0 != result) {
			System.out.println(">>>>>> RouteComputeJob failed.");
			throw new RuntimeException("RouteComputeJob failed.");
		}
		d2 = System.nanoTime();
		bmd.setFlightDelayJob((double) ((d2 - d1) / 1000000));

	}

	/**
	 * To clear the existed output file
	 */
	private static void cleanOutDir(String loc, Configuration conf) throws IOException {
		Path outDirPath = new Path(loc);
		FileSystem fs = FileSystem.get(outDirPath.toUri(), conf);
		fs.delete(outDirPath, true);
	}

}
