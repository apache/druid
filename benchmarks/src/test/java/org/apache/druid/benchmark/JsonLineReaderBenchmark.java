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
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
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
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
@Fork(value = 1)
public class JsonLineReaderBenchmark
{
  private static final int NUM_EVENTS = 1000;

  InputEntityReader reader;
  JsonInputFormat format;
  byte[] data;

  @Setup(Level.Invocation)
  public void prepareReader()
  {
    ByteEntity source = new ByteEntity(data);
    reader = format.createReader(
            new InputRowSchema(
                    new TimestampSpec("timestamp", "iso", null),
                    new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))),
                    ColumnsFilter.all()
            ),
            source,
            null
    );
  }

  @Setup
  public void prepareData() throws Exception
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    String dataString = "{" +
            "\"stack\":\"mainstack\"," +
            "\"metadata\":" +
            "{" +
            "\"application\":\"applicationname\"," +
            "\"detail\":\"tm\"," +
            "\"id\":\"123456789012345678901234567890346973eb4c30eca8a4df79c8219d152cfe0d7d6bdb11a12e609c0c\"," +
            "\"idtwo\":\"123456789012345678901234567890346973eb4c30eca8a4df79c8219d152cfe0d7d6bdb11a12e609c0c\"," +
            "\"sequence\":\"v008\"," +
            "\"stack\":\"mainstack\"," +
            "\"taskId\":\"12345678-1234-1234-1234-1234567890ab\"," +
            "\"taskIdTwo\":\"12345678-1234-1234-1234-1234567890ab\"" +
            "}," +
            "\"_cluster_\":\"kafka\"," +
            "\"_id_\":\"12345678-1234-1234-1234-1234567890ab\"," +
            "\"_offset_\":12111398526," +
            "\"type\":\"CUMULATIVE_DOUBLE\"," +
            "\"version\":\"v1\"," +
            "\"timestamp\":1670425782281," +
            "\"point\":{\"seconds\":1670425782,\"nanos\":217000000,\"value\":0}," +
            "\"_kafka_timestamp_\":1670425782304," +
            "\"_partition_\":60," +
            "\"ec2_instance_id\":\"i-1234567890\"," +
            "\"name\":\"packets_received\"," +
            "\"_topic_\":\"test_topic\"}";
    for (int i = 0; i < NUM_EVENTS; i++) {
      baos.write(StringUtils.toUtf8(dataString));
      baos.write(new byte[]{'\n'});
    }

    data = baos.toByteArray();
  }

  @Setup
  public void prepareFormat()
  {
    format = new JsonInputFormat(
            new JSONPathSpec(
                    true,
                    ImmutableList.of(
                            new JSONPathFieldSpec(JSONPathFieldType.ROOT, "root_baz", "baz"),
                            new JSONPathFieldSpec(JSONPathFieldType.ROOT, "root_baz2", "baz2"),
                            new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg", "$.o.mg"),
                            new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg2", "$.o.mg2"),
                            new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq_omg", ".o.mg"),
                            new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq_omg2", ".o.mg2")
                    )
            ),
            null,
            null,
            null,
            null
    );
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
        .include(JsonLineReaderBenchmark.class.getSimpleName())
        .build();

    new Runner(opt).run();
  }
}
