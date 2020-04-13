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

package org.apache.druid.query.aggregation.datasketches.tuple;

import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;
import org.apache.druid.java.util.common.StringUtils;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/** This is used for generating test data for {@link ArrayOfDoublesSketchAggregationTest} */
class GenerateTestData
{

  public static void main(String[] args) throws Exception
  {
    generateSketches();
    generateBucketTestData();
  }

  private static void generateSketches() throws Exception
  {
    Path path = FileSystems.getDefault().getPath("array_of_doubles_sketch_data.tsv");
    try (BufferedWriter out = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
      Random rand = ThreadLocalRandom.current();
      int key = 0;
      for (int i = 0; i < 20; i++) {
        ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(1024)
            .build();
        sketch.update(key++, new double[] {1});
        sketch.update(key++, new double[] {1});
        out.write("2015010101");
        out.write('\t');
        out.write("product_" + (rand.nextInt(10) + 1));
        out.write('\t');
        out.write(StringUtils.encodeBase64String(sketch.compact().toByteArray()));
        out.newLine();
      }
    }
  }

  // Data for two buckets: test and control.
  // Each user ID is associated with a numeric parameter
  // randomly drawn from normal distribution.
  // Buckets have different means.
  private static void generateBucketTestData() throws Exception
  {
    double meanTest = 10;
    double meanControl = 10.2;
    Path path = FileSystems.getDefault().getPath("bucket_test_data.tsv");
    try (BufferedWriter out = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
      Random rand = ThreadLocalRandom.current();
      for (int i = 0; i < 1000; i++) {
        writeBucketTestRecord(out, "test", i, rand.nextGaussian() + meanTest);
        writeBucketTestRecord(out, "control", i, rand.nextGaussian() + meanControl);
      }
    }
  }

  private static void writeBucketTestRecord(BufferedWriter out, String label, int id, double parameter) throws Exception
  {
    out.write("20170101");
    out.write("\t");
    out.write(label);
    out.write("\t");
    out.write(Integer.toString(id));
    out.write("\t");
    out.write(Double.toString(parameter));
    out.newLine();
  }

}
