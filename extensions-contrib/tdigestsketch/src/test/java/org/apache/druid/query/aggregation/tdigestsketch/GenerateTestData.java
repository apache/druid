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

package org.apache.druid.query.aggregation.tdigestsketch;

import com.tdunning.math.stats.MergingDigest;
import org.apache.druid.java.util.common.StringUtils;

import java.io.BufferedWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This class is largely a copy of GenerateTestData class for Datasketches project.
 * It is used for generating test data for {@link TDigestSketchAggregatorTest}.
 */
public class GenerateTestData
{

  public static void main(String[] args) throws Exception
  {
    Path buildPath = FileSystems.getDefault().getPath("doubles_build_data.tsv");
    Path sketchPath = FileSystems.getDefault().getPath("doubles_sketch_data.tsv");
    BufferedWriter buildData = Files.newBufferedWriter(buildPath, StandardCharsets.UTF_8);
    BufferedWriter sketchData = Files.newBufferedWriter(sketchPath, StandardCharsets.UTF_8);
    Random rand = ThreadLocalRandom.current();
    int sequenceNumber = 0;
    for (int i = 0; i < 20; i++) {
      int product = rand.nextInt(10);
      MergingDigest sketch = new MergingDigest(100);
      for (int j = 0; j < 20; j++) {
        double value = rand.nextDouble();
        buildData.write("2016010101");
        buildData.write('\t');
        buildData.write(Integer.toString(sequenceNumber)); // dimension with unique numbers for ingesting raw data
        buildData.write('\t');
        buildData.write(Integer.toString(product)); // product dimension
        buildData.write('\t');
        buildData.write(Double.toString(value));
        buildData.newLine();
        sketch.add(value);
        sequenceNumber++;
      }
      sketchData.write("2016010101");
      sketchData.write('\t');
      sketchData.write(Integer.toString(product)); // product dimension
      sketchData.write('\t');
      byte[] bytes = new byte[sketch.byteSize()];
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      sketch.asBytes(buffer);
      sketchData.write(StringUtils.encodeBase64String(buffer.array()));
      sketchData.newLine();
    }
    buildData.close();
    sketchData.close();
  }

}
