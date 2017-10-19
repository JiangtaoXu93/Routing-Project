/*
package org.neu.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import quickml.data.AttributesMap;
import quickml.data.PredictionMap;
import quickml.data.instances.ClassifierInstance;
import quickml.supervised.ensembles.randomForest.randomDecisionForest.RandomDecisionForest;
import quickml.supervised.ensembles.randomForest.randomDecisionForest.RandomDecisionForestBuilder;
import quickml.supervised.tree.decisionTree.DecisionTreeBuilder;

public class PredictionUtil {

  private static RandomDecisionForest randomForest = null;

  public static void trainRandomForest(Configuration conf, String trainDataLoc,
      Map<String, Integer> features)
      throws IOException {
    List<ClassifierInstance> instances = getClassifierInstances(conf, trainDataLoc, features);
    randomForest = new RandomDecisionForestBuilder<>(
        new DecisionTreeBuilder<>()).buildPredictiveModel(instances);
  }

  public static void predictRandomForest(Configuration conf, String testDataLoc,
      Map<String, Integer> features)
      throws IOException {
    List<AttributesMap> attributesMaps = getTestAttributes(conf, testDataLoc, features);
    List<PredictionMap> predictionMaps = new ArrayList<>();
    for (AttributesMap am : attributesMaps) {
      predictionMaps.add(randomForest.predict(am));
    }
    System.out.println(predictionMaps);
  }

  private static List<AttributesMap> getTestAttributes(Configuration conf, String testDataLoc,
      Map<String, Integer> features) throws IOException {
    List<AttributesMap> attributesMaps = new ArrayList<>();
    Path testDataPath = new Path(testDataLoc);
    FileSystem fs = FileSystem.get(testDataPath.toUri(), conf);
    FileStatus[] status = fs.listStatus(testDataPath);
    InputStreamReader inputStreamReader = new InputStreamReader(fs.open(
        status[0].getPath()));
    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      String[] splitLine = line.split(",");
      AttributesMap attributes = AttributesMap.newHashMap();
      for (Map.Entry<String, Integer> entry : features.entrySet()) {
        String val = splitLine[entry.getValue()].trim();
        attributes.put(entry.getKey(), val);
      }
      attributesMaps.add(attributes);
    }
    inputStreamReader.close();
    bufferedReader.close();
    return attributesMaps;
  }

  private static List<ClassifierInstance> getClassifierInstances(Configuration conf,
      String trainDataLoc,
      Map<String, Integer> features) throws IOException {
    List<ClassifierInstance> instances = new ArrayList<>();
    Path trainDataPath = new Path(trainDataLoc);
    FileSystem fs = FileSystem.get(trainDataPath.toUri(), conf);
    FileStatus[] status = fs.listStatus(trainDataPath);
    InputStreamReader inputStreamReader = new InputStreamReader(fs.open(
        status[0].getPath()));
    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      String[] splitLine = line.split(",");
      AttributesMap attributes = AttributesMap.newHashMap();

      for (Map.Entry<String, Integer> entry : features.entrySet()) {
        String val = splitLine[entry.getValue()].trim();
        attributes.put(entry.getKey(), val);
      }
      int label = Integer.parseInt(splitLine[splitLine.length - 1]);
      instances.add(new ClassifierInstance(attributes, label));
    }
    inputStreamReader.close();
    bufferedReader.close();
    return instances;
  }
}
*/
