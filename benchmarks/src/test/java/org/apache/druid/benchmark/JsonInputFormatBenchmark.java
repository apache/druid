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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.JsonLineReader;
import org.apache.druid.data.input.impl.JsonNodeReader;
import org.apache.druid.data.input.impl.JsonReader;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.input.InputRowSchemas;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.transform.TransformSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Tests {@link JsonInputFormat} delegates, one per {@link ReaderType}.
 *
 * Output is in nanoseconds per parse (or parse and read) of {@link #DATA_STRING}.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(JsonInputFormatBenchmark.NUM_EVENTS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(value = 1)
public class JsonInputFormatBenchmark
{
  enum ReaderType
  {
    READER(JsonReader.class) {
      @Override
      public JsonInputFormat createFormat(JSONPathSpec flattenSpec)
      {
        return new JsonInputFormat(flattenSpec, null, null, false, false).withLineSplittable(false);
      }
    },
    LINE_READER(JsonLineReader.class) {
      @Override
      public JsonInputFormat createFormat(JSONPathSpec flattenSpec)
      {
        return new JsonInputFormat(flattenSpec, null, null, null, null).withLineSplittable(true);
      }
    },
    NODE_READER(JsonNodeReader.class) {
      @Override
      public JsonInputFormat createFormat(JSONPathSpec flattenSpec)
      {
        return new JsonInputFormat(flattenSpec, null, null, false, true).withLineSplittable(false);
      }
    };

    private final Class<? extends InputEntityReader> clazz;

    ReaderType(Class<? extends InputEntityReader> clazz)
    {
      this.clazz = clazz;
    }

    public abstract JsonInputFormat createFormat(JSONPathSpec flattenSpec);
  }

  public static final int NUM_EVENTS = 1000;

  private static final String DATA_STRING =
      "{" +
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

  private static final List<String> FIELDS_TO_READ =
      ImmutableList.of(
          "stack",
          "_cluster_",
          "_id_",
          "_offset_",
          "type",
          "version",
          "_kafka_timestamp_",
          "_partition_",
          "ec2_instance_id",
          "name",
          "_topic",
          "root_type",
          "path_app",
          "jq_app"
      );

  ReaderType readerType;
  InputRowSchema inputRowSchema;
  InputEntityReader reader;
  JsonInputFormat format;
  List<Function<InputRow, Object>> fieldFunctions;
  ByteEntity data;

  @Param({"reader", "node_reader", "line_reader"})
  private String readerTypeString;

  /**
   * If false: only read {@link #FIELDS_TO_READ}. If true: discover and read all fields.
   */
  @Param({"false", "true"})
  private boolean discovery;

  @Setup
  public void setUpTrial() throws Exception
  {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final byte[] dataUtf8 = StringUtils.toUtf8(DATA_STRING);

    for (int i = 0; i < NUM_EVENTS; i++) {
      baos.write(dataUtf8);
      baos.write(new byte[]{'\n'});
    }

    final TimestampSpec timestampSpec = new TimestampSpec("timestamp", "iso", null);
    final DimensionsSpec dimensionsSpec;

    if (discovery) {
      // Discovered schema, excluding uninteresting fields that are not in FIELDS_TO_READ.
      final Set<String> exclusions = Sets.difference(
          TestHelper.makeJsonMapper()
                    .readValue(DATA_STRING, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT)
                    .keySet(),
          ImmutableSet.copyOf(FIELDS_TO_READ)
      );

      dimensionsSpec = DimensionsSpec.builder()
                                     .useSchemaDiscovery(true)
                                     .setDimensionExclusions(ImmutableList.copyOf(exclusions))
                                     .build();
    } else {
      // Fully defined schema.
      dimensionsSpec = DimensionsSpec.builder()
                                     .setDimensions(DimensionsSpec.getDefaultSchemas(FIELDS_TO_READ))
                                     .build();
    }

    data = new ByteEntity(baos.toByteArray());
    readerType = ReaderType.valueOf(StringUtils.toUpperCase(readerTypeString));
    inputRowSchema = new InputRowSchema(
        timestampSpec,
        dimensionsSpec,
        InputRowSchemas.createColumnsFilter(
            timestampSpec,
            dimensionsSpec,
            TransformSpec.NONE,
            new AggregatorFactory[0]
        )
    );
    format = readerType.createFormat(
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.ROOT, "root_type", "type"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_app", "$.metadata.application"),
                new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq_app", ".metadata.application")
            )
        )
    );

    final RowAdapter<InputRow> rowAdapter = format.createRowAdapter(inputRowSchema);
    fieldFunctions = new ArrayList<>(FIELDS_TO_READ.size());

    for (final String field : FIELDS_TO_READ) {
      fieldFunctions.add(rowAdapter.columnFunction(field));
    }

    reader = format.createReader(inputRowSchema, data, null);

    if (reader.getClass() != readerType.clazz) {
      throw new ISE(
          "Expected class[%s] for readerType[%s], got[%s]",
          readerType.clazz,
          readerTypeString,
          reader.getClass()
      );
    }
  }

  /**
   * Benchmark parsing, but not reading fields.
   */
  @Benchmark
  public void parse(final Blackhole blackhole) throws IOException
  {
    data.getBuffer().rewind();

    int counted = 0;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      while (iterator.hasNext()) {
        final InputRow row = iterator.next();
        if (row != null) {
          counted += 1;
          blackhole.consume(row);
        }
      }
    }

    if (counted != NUM_EVENTS) {
      throw new RuntimeException("invalid number of loops, counted = " + counted);
    }
  }

  /**
   * Benchmark parsing and reading {@link #FIELDS_TO_READ}. More realistic than {@link #parse(Blackhole)}.
   */
  @Benchmark
  public void parseAndRead(final Blackhole blackhole) throws IOException
  {
    data.getBuffer().rewind();

    int counted = 0;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      while (iterator.hasNext()) {
        final InputRow row = iterator.next();
        if (row != null) {
          for (Function<InputRow, Object> fieldFunction : fieldFunctions) {
            blackhole.consume(fieldFunction.apply(row));
          }

          counted += 1;
        }
      }
    }

    if (counted != NUM_EVENTS) {
      throw new RuntimeException("invalid number of loops, counted = " + counted);
    }
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(JsonInputFormatBenchmark.class.getSimpleName())
        .build();

    new Runner(opt).run();
  }
}
