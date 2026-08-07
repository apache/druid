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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.regex.RegexConfig;
import org.apache.druid.regex.RegexEngineType;
import org.apache.druid.utils.CompressionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class RegexInputFormatTest
{
  private final ObjectMapper mapper;

  public RegexInputFormatTest()
  {
    mapper = new ObjectMapper();
    mapper.registerSubtypes(new NamedType(RegexInputFormat.class, "regex"));
  }

  @ParameterizedTest
  @EnumSource(RegexEngineType.class)
  public void testSerde(RegexEngineType engineType) throws IOException
  {
    final RegexConfig regexConfig = RegexConfig.with(engineType);
    mapper.setInjectableValues(new InjectableValues.Std().addValue(RegexConfig.class, regexConfig));

    final RegexInputFormat expected = new RegexInputFormat(
        regexConfig,
        "//[^\\r\\n]*[\\r\\n]",
        "|",
        ImmutableList.of("col1", "col2", "col3")
    );

    final byte[] json = mapper.writeValueAsBytes(expected);
    final RegexInputFormat fromJson = (RegexInputFormat) mapper.readValue(json, InputFormat.class);

    Assertions.assertEquals(expected.getPattern(), fromJson.getPattern());
    Assertions.assertEquals(expected.getListDelimiter(), fromJson.getListDelimiter());
    Assertions.assertEquals(expected.getColumns(), fromJson.getColumns());
  }

  @ParameterizedTest
  @EnumSource(RegexEngineType.class)
  public void testIgnoreCompiledPatternInJson(RegexEngineType engineType) throws IOException
  {
    final RegexInputFormat expected = new RegexInputFormat(
        RegexConfig.with(engineType),
        "//[^\\r\\n]*[\\r\\n]",
        "|",
        ImmutableList.of("col1", "col2", "col3")
    );

    final byte[] json = mapper.writeValueAsBytes(expected);
    final Map<String, Object> map = mapper.readValue(json, Map.class);
    Assertions.assertFalse(map.containsKey("compiledPattern"));
  }

  @ParameterizedTest
  @EnumSource(RegexEngineType.class)
  public void test_getWeightedSize_withoutCompression(RegexEngineType engineType)
  {
    final RegexInputFormat format = new RegexInputFormat(
        RegexConfig.with(engineType),
        "//[^\\r\\n]*[\\r\\n]",
        "|",
        ImmutableList.of("col1", "col2", "col3")
    );
    final long unweightedSize = 100L;
    Assertions.assertEquals(unweightedSize, format.getWeightedSize("file.txt", unweightedSize));

  }

  @ParameterizedTest
  @EnumSource(RegexEngineType.class)
  public void test_getWeightedSize_withGzCompression(RegexEngineType engineType)
  {
    final RegexInputFormat format = new RegexInputFormat(
        RegexConfig.with(engineType),
        "//[^\\r\\n]*[\\r\\n]",
        "|",
        ImmutableList.of("col1", "col2", "col3")
    );
    final long unweightedSize = 100L;
    Assertions.assertEquals(
        unweightedSize * CompressionUtils.COMPRESSED_TEXT_WEIGHT_FACTOR,
        format.getWeightedSize("file.txt.gz", unweightedSize)
    );
  }

  @Test
  @Timeout(10)
  public void test_backtracking() throws IOException
  {
    final RegexInputFormat inputFormat = new RegexInputFormat(
        RegexConfig.with(RegexEngineType.RE2J),
        "^(.*a){20}$",
        null,
        ImmutableList.of("value")
    );

    String maliciousInput = "a".repeat(50) + "X";
    InputEntityReader reader = inputFormat.createReader(
        null,
        new ByteEntity(maliciousInput.getBytes(StandardCharsets.UTF_8)),
        null
    );

    try (CloseableIterator<?> iterator = reader.read()) {
      while (iterator.hasNext()) {
        iterator.next();
      }
    }

    catch (ParseException ignored) {
      // expected for non-matching input
    }
  }
}
