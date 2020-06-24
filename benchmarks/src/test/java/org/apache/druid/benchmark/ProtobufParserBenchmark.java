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

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.protobuf.ProtobufInputRowParser;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class ProtobufParserBenchmark
{
  @Param({"75000"})
  private int rowsPerSegment;

  private static final Logger log = new Logger(ProtobufParserBenchmark.class);

  static {
    NullHandling.initializeForTests();
  }

  private ParseSpec nestedParseSpec;
  private ProtobufInputRowParser nestedParser;
  private ParseSpec flatParseSpec;
  private ProtobufInputRowParser flatParser;
  private byte[] protoInputs;
  private String protoFilePath;

  @Setup
  public void setup()
  {
    nestedParseSpec = new JSONParseSpec(
                new TimestampSpec("timestamp", "iso", null),
                new DimensionsSpec(Lists.newArrayList(
                        new StringDimensionSchema("event"),
                        new StringDimensionSchema("id"),
                        new StringDimensionSchema("someOtherId"),
                        new StringDimensionSchema("isValid")
                ), null, null),
                new JSONPathSpec(
                        true,
                        Lists.newArrayList(
                                new JSONPathFieldSpec(JSONPathFieldType.ROOT, "eventType", "eventType"),
                                new JSONPathFieldSpec(JSONPathFieldType.PATH, "foobar", "$.foo.bar"),
                                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bar0", "$.bar[0].bar")
                        )
                ),
                null,
                null
    );

    flatParseSpec = new JSONParseSpec(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(Lists.newArrayList(
                    new StringDimensionSchema("event"),
                    new StringDimensionSchema("id"),
                    new StringDimensionSchema("someOtherId"),
                    new StringDimensionSchema("isValid")
            ), null, null),
            null,
            null,
            null
    );

    protoFilePath = "ProtoFile";
    protoInputs = getProtoInputs(protoFilePath);
    nestedParser = new ProtobufInputRowParser(nestedParseSpec, "prototest.desc", "ProtoTestEvent");
    flatParser = new ProtobufInputRowParser(flatParseSpec, "prototest.desc", "ProtoTestEvent");
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void consumeFlatData(Blackhole blackhole)
  {
    for (int i = 0; i < rowsPerSegment; i++) {
      InputRow row = flatParser.parseBatch(ByteBuffer.wrap(protoInputs)).get(0);
      blackhole.consume(row);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void consumeNestedData(Blackhole blackhole)
  {
    for (int i = 0; i < rowsPerSegment; i++) {
      InputRow row = nestedParser.parseBatch(ByteBuffer.wrap(protoInputs)).get(0);
      blackhole.consume(row);
    }

  }
  private byte[] getProtoInputs(String fileName)
  {
    String filePath = this.getClass().getClassLoader().getResource(fileName).getPath();
    byte[] bytes = null;
    try {
      File file = new File(filePath);
      bytes = new byte[(int) file.length()];
      bytes = Files.toByteArray(file);
    }
    catch (FileNotFoundException e) {
      log.error("Cannot find the file: " + filePath);
      e.printStackTrace();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    return bytes;
  }
}
