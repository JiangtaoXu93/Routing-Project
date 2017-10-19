package org.neu.job;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.neu.util.PredictionUtil;

public class RoutePredictionJob extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    Map<String, Integer> features = createFeatureMap();

    PredictionUtil.trainRandomForest(getConf(), args[3] + "/route/train-r-00000", features);
    PredictionUtil.predictRandomForest(getConf(), args[3] + "/route/test-r-00000", features);

    return 0;
  }

  private Map<String, Integer> createFeatureMap() {
    Map<String, Integer> featureMap = new HashMap<>();
    featureMap.put("origin", 1);
    featureMap.put("destination", 1);
    return featureMap;
  }
}
