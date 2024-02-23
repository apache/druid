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

package org.apache.druid.benchmark;

import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(value = 1)
public class DelimitedInputFormatBenchmark
{
  private static final int NUM_EVENTS = 1200;

  private static final List<String> COLUMNS =
      ImmutableList.of(
          "timestamp",
          "page",
          "language",
          "user",
          "unpatrolled",
          "newPage",
          "robot",
          "anonymous",
          "namespace",
          "continent",
          "country",
          "region",
          "city",
          "added",
          "deleted",
          "delta"
      );

  static {
    NullHandling.initializeForTests();
  }

  @Param({"false", "true"})
  private boolean fromHeader;

  InputEntityReader reader;
  DelimitedInputFormat format;
  byte[] data;

  @Setup(Level.Invocation)
  public void prepareReader()
  {
    ByteEntity source = new ByteEntity(data);
    reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            DimensionsSpec.builder().setIncludeAllDimensions(true).build(),
            ColumnsFilter.all()
        ),
        source,
        null
    );
  }

  @Setup(Level.Trial)
  public void prepareData() throws Exception
  {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final String headerString =
        "timestamp\tpage\tlanguage\tuser\tunpatrolled\tnewPage\trobot\tanonymous\tnamespace\tcontinent\tcountry\tregion\tcity\tadded\tdeleted\tdelta\n";
    final String dataString =
        "2013-08-31T01:02:33Z\tGypsy Danger\ten\tnuclear\ttrue\ttrue\tfalse\tfalse\tarticle\tNorth America\tUnited States\tBay Area\tSan Francisco\t57\t200\t-143\n"
        + "2013-08-31T03:32:45Z\tStriker Eureka\ten\tspeed\tfalse\ttrue\ttrue\tfalse\twikipedia\tAustralia\tAustralia\tCantebury\tSyndey\t459\t129\t330\n"
        + "2013-08-31T07:11:21Z\tCherno Alpha\tru\tmasterYi\tfalse\ttrue\ttrue\tfalse\tarticle\tAsia\tRussia\tOblast\tMoscow\t123\t12\t111\n";

    baos.write(StringUtils.toUtf8(headerString));

    for (int i = 0; i < NUM_EVENTS / 3; i++) {
      baos.write(StringUtils.toUtf8(dataString));
    }

    data = baos.toByteArray();
  }

  @Setup(Level.Trial)
  public void prepareFormat()
  {
    format = new DelimitedInputFormat(fromHeader ? null : COLUMNS, null, "\t", null, fromHeader, fromHeader ? 0 : 1);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void baseline(final Blackhole blackhole) throws IOException
  {
    int counted = 0;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      while (iterator.hasNext()) {
        final InputRow row = iterator.next();
        if (row != null) {
          counted += 1;
        }
        blackhole.consume(row);
      }
    }

    if (counted != NUM_EVENTS) {
      throw new RuntimeException("invalid number of loops, counted = " + counted);
    }
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(DelimitedInputFormatBenchmark.class.getSimpleName())
        .build();

    new Runner(opt).run();
  }
}
