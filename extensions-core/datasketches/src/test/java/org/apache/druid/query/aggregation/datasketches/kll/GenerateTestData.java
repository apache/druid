/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.aggregation.datasketches.kll;

import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.druid.java.util.common.StringUtils;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/** This is used for generating test data for {@link KllDoublesSketchAggregatorTest} */
public class GenerateTestData
{

  public static void main(String[] args) throws Exception
  {
    final Path doublesBuildPath = FileSystems.getDefault().getPath("kll_doubles_sketch_build_data.tsv");
    final Path doublesSketchPath = FileSystems.getDefault().getPath("kll_doubles_sketch_data.tsv");
    final Path floatsBuildPath = FileSystems.getDefault().getPath("kll_floats_sketch_build_data.tsv");
    final Path floatsSketchPath = FileSystems.getDefault().getPath("kll_floats_sketch_data.tsv");
    final BufferedWriter doublesBuildData = Files.newBufferedWriter(doublesBuildPath, StandardCharsets.UTF_8);
    final BufferedWriter doublesSketchData = Files.newBufferedWriter(doublesSketchPath, StandardCharsets.UTF_8);
    final BufferedWriter floatsBuildData = Files.newBufferedWriter(floatsBuildPath, StandardCharsets.UTF_8);
    final BufferedWriter floatsSketchData = Files.newBufferedWriter(floatsSketchPath, StandardCharsets.UTF_8);
    Random rand = ThreadLocalRandom.current();
    int sequenceNumber = 0;
    for (int i = 0; i < 20; i++) {
      int product = rand.nextInt(10);
      KllDoublesSketch doublesSketch = KllDoublesSketch.newHeapInstance();
      KllFloatsSketch floatsSketch = KllFloatsSketch.newHeapInstance();
      for (int j = 0; j < 20; j++) {
        double value = rand.nextDouble();
        doublesBuildData.write("2016010101");
        doublesBuildData.write('\t');
        doublesBuildData.write(Integer.toString(sequenceNumber)); // dimension with unique numbers for ingesting raw data
        doublesBuildData.write('\t');
        doublesBuildData.write(Integer.toString(product)); // product dimension
        doublesBuildData.write('\t');
        doublesBuildData.write(Double.toString(value));
        floatsBuildData.write("2016010101");
        floatsBuildData.write('\t');
        floatsBuildData.write(Integer.toString(sequenceNumber)); // dimension with unique numbers for ingesting raw data
        floatsBuildData.write('\t');
        floatsBuildData.write(Integer.toString(product)); // product dimension
        floatsBuildData.write('\t');
        floatsBuildData.write(Float.toString((float) value));
        if (rand.nextFloat() > 0.1) { // 10% nulls in this field
          doublesBuildData.write('\t');
          doublesBuildData.write(Double.toString(value * 5 + 5));
          floatsBuildData.write('\t');
          floatsBuildData.write(Float.toString((float) (value * 5 + 5)));
        }
        doublesBuildData.newLine();
        floatsBuildData.newLine();
        doublesSketch.update(value);
        floatsSketch.update((float) value);
        sequenceNumber++;
      }
      doublesSketchData.write("2016010101");
      doublesSketchData.write('\t');
      doublesSketchData.write(Integer.toString(product)); // product dimension
      doublesSketchData.write('\t');
      doublesSketchData.write(StringUtils.encodeBase64String(doublesSketch.toByteArray()));
      doublesSketchData.newLine();
      floatsSketchData.write("2016010101");
      floatsSketchData.write('\t');
      floatsSketchData.write(Integer.toString(product)); // product dimension
      floatsSketchData.write('\t');
      floatsSketchData.write(StringUtils.encodeBase64String(floatsSketch.toByteArray()));
      floatsSketchData.newLine();
    }
    doublesBuildData.close();
    doublesSketchData.close();
    floatsBuildData.close();
    floatsSketchData.close();
  }

}
