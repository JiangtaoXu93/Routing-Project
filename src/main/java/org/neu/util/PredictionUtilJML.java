package org.neu.util;

import java.util.Map;
import net.sf.javaml.classification.Classifier;
import net.sf.javaml.core.Dataset;
import org.apache.hadoop.conf.Configuration;

public class PredictionUtilJML {

  private static Classifier randomForest = null;

  public static void trainRandomForest(Configuration conf, String trainDataLoc,
      Map<String, Integer> features) {

//    Dataset trainData = loadTrainData(conf,trainDataLoc)

  }
}
