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

package org.apache.druid.segment.realtime.firehose;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.impl.prefetch.JsonIterator;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.segment.transform.TransformingStringInputRowParser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SqlFirehoseTest
{
  private static final TypeReference<Map<String, Object>> TYPE_REF = new TypeReference<Map<String, Object>>()
  {
  };
  private static File TEST_DIR;
  private List<Map<String, Object>> inputs;
  private List<FileInputStream> fileList;
  private InputRowParser parser;
  private ObjectMapper objectMapper;


  @Before
  public void setup() throws IOException
  {
    TEST_DIR = File.createTempFile(SqlFirehose.class.getSimpleName(), "testDir");
    FileUtils.forceDelete(TEST_DIR);
    FileUtils.forceMkdir(TEST_DIR);

    final List<Map<String, Object>> inputTexts = ImmutableList.of(
        ImmutableMap.of("x", "foostring1", "timestamp", 2000),
        ImmutableMap.of("x", "foostring2", "timestamp", 2000)
    );
    List<FileInputStream> testFile = new ArrayList<>();
    this.objectMapper = new ObjectMapper(new SmileFactory());
    int i = 0;
    for (Map m : inputTexts) {
      File file = new File(TEST_DIR, "test_" + i++);
      try (FileOutputStream fos = new FileOutputStream(file)) {
        final JsonGenerator jg = objectMapper.getFactory().createGenerator(fos);
        jg.writeStartArray();
        jg.writeObject(m);
        jg.writeEndArray();
        jg.close();
        testFile.add(new FileInputStream(file));
      }
    }

    this.fileList = testFile;
    parser = TransformSpec.NONE.decorate(
        new MapInputRowParser(
            new TimeAndDimsParseSpec(
                new TimestampSpec("timestamp", "auto", null),
                new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("x")), null, null)
            )
        )
    );

    this.inputs = inputTexts;
  }

  @Test
  public void testFirehose() throws Exception
  {
    final TestCloseable closeable = new TestCloseable();
    List<Object> expectedResults = new ArrayList<>();
    for (Map<String, Object> map : inputs) {
      expectedResults.add(map.get("x"));
    }
    final List<JsonIterator<Map<String, Object>>> lineIterators =
        fileList.stream()
                .map(s -> new JsonIterator<Map<String, Object>>(TYPE_REF, s, closeable, objectMapper))
                .collect(Collectors.toList());

    try (final SqlFirehose firehose = new SqlFirehose(lineIterators.iterator(), parser, closeable)) {
      final List<Object> results = new ArrayList<>();

      while (firehose.hasMore()) {
        final InputRow inputRow = firehose.nextRow();
        if (inputRow == null) {
          results.add(null);
        } else {
          results.add(inputRow.getDimension("x").get(0));
        }
      }

      Assert.assertEquals(expectedResults, results);
    }
  }

  @Test
  public void testFirehoseStringParser() throws Exception
  {
    final TestCloseable closeable = new TestCloseable();
    List<Object> expectedResults = new ArrayList<>();
    for (Map<String, Object> map : inputs) {
      expectedResults.add(map.get("x"));
    }

    final List<JsonIterator<Map<String, Object>>> lineIterators =
        fileList.stream()
                .map(s -> new JsonIterator<Map<String, Object>>(TYPE_REF, s, closeable, objectMapper))
                .collect(Collectors.toList());

    final InputRowParser stringParser = TransformSpec.NONE.decorate(
        new StringInputRowParser(
          new TimeAndDimsParseSpec(
              new TimestampSpec("timestamp", "auto", null),
              new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("x")), null, null)
          ),
          Charset.defaultCharset().name()
        )
    );
    try (final SqlFirehose firehose = new SqlFirehose(lineIterators.iterator(), stringParser, closeable)) {
      final List<Object> results = new ArrayList<>();

      while (firehose.hasMore()) {
        final InputRow inputRow = firehose.nextRow();
        if (inputRow == null) {
          results.add(null);
        } else {
          results.add(inputRow.getDimension("x").get(0));
        }
      }

      Assert.assertEquals(expectedResults, results);
    }
  }

  @Test
  public void testFirehoseTransformingParser() throws Exception
  {
    final TestCloseable closeable = new TestCloseable();
    List<Object> expectedResults = new ArrayList<>();
    for (Map<String, Object> map : inputs) {
      expectedResults.add(map.get("x") + "foo");
    }

    final List<JsonIterator<Map<String, Object>>> lineIterators =
        fileList.stream()
                .map(s -> new JsonIterator<Map<String, Object>>(TYPE_REF, s, closeable, objectMapper))
                .collect(Collectors.toList());

    final InputRowParser stringParser = new TransformingStringInputRowParser(
        new TimeAndDimsParseSpec(
            new TimestampSpec("timestamp", "auto", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("x")), null, null)
        ),
        Charset.defaultCharset().name(),
        new TransformSpec(
            null,
            ImmutableList.of(
                new ExpressionTransform("xfoo", "concat(x,'foo')", ExprMacroTable.nil())
            )
        )
    );

    try (final SqlFirehose firehose = new SqlFirehose(lineIterators.iterator(), stringParser, closeable)) {
      final List<Object> results = new ArrayList<>();

      while (firehose.hasMore()) {
        final InputRow inputRow = firehose.nextRow();
        if (inputRow == null) {
          results.add(null);
        } else {
          results.add(inputRow.getDimension("xfoo").get(0));
        }
      }

      Assert.assertEquals(expectedResults, results);
    }
  }

  @Test
  public void testClose() throws IOException
  {
    File file = File.createTempFile("test", "", TEST_DIR);
    final TestCloseable closeable = new TestCloseable();
    try (FileOutputStream fos = new FileOutputStream(file)) {
      final JsonGenerator jg = objectMapper.getFactory().createGenerator(fos);
      jg.writeStartArray();
      jg.writeEndArray();
      jg.close();
    }

    final JsonIterator<Map<String, Object>> jsonIterator = new JsonIterator<>(
        TYPE_REF,
        new FileInputStream(file),
        closeable,
        objectMapper
    );

    final SqlFirehose firehose = new SqlFirehose(
        ImmutableList.of(jsonIterator).iterator(),
        parser,
        closeable
    );
    firehose.hasMore(); // initialize lineIterator
    firehose.close();
    Assert.assertTrue(closeable.closed);
  }

  @After
  public void teardown() throws IOException
  {
    FileUtils.forceDelete(TEST_DIR);
  }

  private static final class TestCloseable implements Closeable
  {
    private boolean closed;

    @Override
    public void close()
    {
      closed = true;
    }
  }
}

