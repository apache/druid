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

package org.apache.druid.benchmark.compression;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.generator.ColumnValueGenerator;
import org.apache.druid.segment.generator.GeneratorColumnSchema;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.File;
import java.io.IOException;
import java.util.List;

@State(Scope.Benchmark)
public class BaseColumnarLongsFromGeneratorBenchmark extends BaseColumnarLongsBenchmark
{
  static int SEED = 1;

  /**
   * Controls the probability that any generated value will be a zero, to simulate sparely populated columns
   */
  @Param({
      "0.0",
      "0.25",
      "0.5",
      "0.75",
      "0.95"
  })
  double zeroProbability;

  /**
   * Number of rows generated for the value distribution
   */
  @Param({"5000000"})
  int rows;

  /**
   * Value distributions to simulate various patterns of long column
   */
  @Param({
      "enumerated-0-1",
      "enumerated-full",
      "normal-1-32",
      "normal-40-1000",
      "sequential-1000",
      "sequential-unique",
      "uniform-1",
      "uniform-2",
      "uniform-3",
      "uniform-4",
      "uniform-8",
      "uniform-12",
      "uniform-16",
      "uniform-20",
      "uniform-24",
      "uinform-32",
      "uniform-40",
      "uniform-48",
      "uniform-56",
      "uniform-64",
      "zipf-low-100",
      "zipf-low-100000",
      "zipf-low-32-bit",
      "zipf-high-100",
      "zipf-high-100000",
      "zipf-high-32-bit"
  })
  String distribution;


  static ColumnValueGenerator makeGenerator(
      String distribution,
      int rows,
      double zeroProbability
  )
  {
    List<Object> enumerated;
    List<Double> probability;
    switch (distribution) {
      case "enumerated-0-1":
        enumerated = ImmutableList.of(0, 1);
        probability = ImmutableList.of(0.6, 0.4);
        return GeneratorColumnSchema.makeEnumerated(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            enumerated,
            probability
        ).makeGenerator(SEED);
      case "enumerated-full":
        enumerated = ImmutableList.of(
            0,
            1,
            Long.MAX_VALUE - 1,
            Long.MIN_VALUE + 1,
            Long.MIN_VALUE / 2,
            Long.MAX_VALUE / 2
        );
        probability = ImmutableList.of(0.4, 0.2, 0.1, 0.1, 0.1, 0.1);
        return GeneratorColumnSchema.makeEnumerated(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            enumerated,
            probability
        ).makeGenerator(SEED);
      case "normal-1-32":
        return GeneratorColumnSchema.makeNormal(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            1.0,
            (double) (1L << 32),
            true
        ).makeGenerator(SEED);
      case "normal-40-1000":
        return GeneratorColumnSchema.makeNormal(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            (double) (1L << 40),
            1000.0,
            true
        ).makeGenerator(SEED);
      case "sequential-1000":
        return GeneratorColumnSchema.makeSequential(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            Integer.MAX_VALUE - 1001,
            Integer.MAX_VALUE - 1
        ).makeGenerator(SEED);
      case "sequential-unique":
        return GeneratorColumnSchema.makeSequential(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            0,
            rows
        ).makeGenerator(SEED);
      case "uniform-1":
        return GeneratorColumnSchema.makeDiscreteUniform(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            0,
            1
        ).makeGenerator(SEED);
      case "uniform-2":
        return GeneratorColumnSchema.makeDiscreteUniform(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            0,
            4
        ).makeGenerator(SEED);
      case "uniform-3":
        return GeneratorColumnSchema.makeDiscreteUniform(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            1000000,
            1000008
        ).makeGenerator(SEED);
      case "uniform-4":
        return GeneratorColumnSchema.makeDiscreteUniform(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            0,
            1 << 4
        ).makeGenerator(SEED);
      case "uniform-8":
        return GeneratorColumnSchema.makeDiscreteUniform(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            0,
            1 << 8
        ).makeGenerator(SEED);
      case "uniform-12":
        return GeneratorColumnSchema.makeDiscreteUniform(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            0,
            1 << 12
        ).makeGenerator(SEED);
      case "uniform-16":
        return GeneratorColumnSchema.makeDiscreteUniform(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            0,
            1 << 16
        ).makeGenerator(SEED);
      case "uniform-20":
        return GeneratorColumnSchema.makeContinuousUniform(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            0,
            1 << 20
        ).makeGenerator(SEED);
      case "uniform-24":
        return GeneratorColumnSchema.makeContinuousUniform(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            0,
            (1 << 24) - 1
        ).makeGenerator(SEED);
      case "uinform-32":
        return GeneratorColumnSchema.makeContinuousUniform(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            0,
            Integer.MAX_VALUE - 1
        ).makeGenerator(SEED);
      case "uniform-40":
        return GeneratorColumnSchema.makeContinuousUniform(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            0L,
            (1L << 40) - 1
        ).makeGenerator(SEED);
      case "uniform-48":
        return GeneratorColumnSchema.makeContinuousUniform(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            0,
            (1L << 48) - 1
        ).makeGenerator(SEED);
      case "uniform-56":
        return GeneratorColumnSchema.makeContinuousUniform(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            0,
            (1L << 56 - 1)
        ).makeGenerator(SEED);
      case "uniform-64":
        return GeneratorColumnSchema.makeContinuousUniform(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            0,
            Long.MAX_VALUE - 1
        ).makeGenerator(SEED);
      case "zipf-low-100":
        return GeneratorColumnSchema.makeLazyZipf(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            0,
            100,
            1d
        ).makeGenerator(SEED);
      case "zipf-low-100000":
        return GeneratorColumnSchema.makeLazyZipf(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            -50000,
            50000,
            1d
        ).makeGenerator(SEED);
      case "zipf-low-32-bit":
        return GeneratorColumnSchema.makeLazyZipf(
            distribution,
            ValueType.LONG,
            true,
            1,
            0d,
            0,
            Integer.MAX_VALUE,
            1d
        ).makeGenerator(SEED);
      case "zipf-high-100":
        return GeneratorColumnSchema.makeLazyZipf(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            0,
            100,
            3d
        ).makeGenerator(SEED);
      case "zipf-high-100000":
        return GeneratorColumnSchema.makeLazyZipf(
            distribution,
            ValueType.LONG,
            true,
            1,
            zeroProbability,
            -50000,
            50000,
            3d
        ).makeGenerator(SEED);
      case "zipf-high-32-bit":
        return GeneratorColumnSchema.makeLazyZipf(
            distribution,
            ValueType.LONG,
            true,
            1,
            0d,
            0,
            Integer.MAX_VALUE,
            3d
        ).makeGenerator(SEED);
    }
    throw new IllegalArgumentException("unknown distribution");
  }

  static String getGeneratorEncodedFilename(String encoding, String distribution, int rows, double nullProbability)
  {
    return StringUtils.format("%s-%s-%s-%s.bin", encoding, distribution, rows, nullProbability);
  }

  static File getTmpDir() throws IOException
  {
    final String dirPath = "tmp/encoding/longs/";
    File dir = new File(dirPath);
    FileUtils.mkdirp(dir);
    return dir;
  }
}
