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

package org.apache.druid.indexing.input;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.IndexingServiceInputSourceModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.generator.DataGenerator;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorColumnSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class GeneratorInputSourceTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    GeneratorInputSource inputSource = new GeneratorInputSource(
        "basic",
        null,
        1000,
        2,
        1024L,
        DateTimes.nowUtc().getMillis(),
        1000,
        1.0
    );

    String serialized = MAPPER.writeValueAsString(inputSource);
    GeneratorInputSource sauce = MAPPER.readValue(serialized, GeneratorInputSource.class);

    Assert.assertEquals(inputSource, sauce);
  }

  @Test
  public void testSerdeWithSchema() throws JsonProcessingException
  {
    GeneratorInputSource inputSource = new GeneratorInputSource(
        null,
        ImmutableList.of(
            GeneratorColumnSchema.makeLazyZipf(
                "test",
                ValueType.LONG,
                false,
                1,
                0.0,
                0,
                1000,
                1.3
            )
        ),
        1000,
        2,
        1024L,
        DateTimes.nowUtc().getMillis(),
        1000,
        1.0
    );

    String serialized = MAPPER.writeValueAsString(inputSource);
    GeneratorInputSource sauce = MAPPER.readValue(serialized, GeneratorInputSource.class);

    Assert.assertEquals(inputSource, sauce);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(GeneratorInputSource.class).usingGetClass().verify();
  }

  @Test
  public void testReader() throws IOException
  {
    final long seed = 1024L;
    final long millis = DateTimes.nowUtc().getMillis();
    final int numConsecutiveTimestamps = 1000;
    final double timestampIncrement = 1.0;
    final int numRows = 1000;
    GeneratorInputSource inputSource = new GeneratorInputSource(
        "basic",
        null,
        numRows,
        2,
        seed,
        millis,
        numConsecutiveTimestamps,
        timestampIncrement
    );

    DataGenerator generator = new DataGenerator(
        GeneratorBasicSchemas.SCHEMA_MAP.get("basic").getColumnSchemas(),
        seed,
        millis,
        numConsecutiveTimestamps,
        timestampIncrement
    );

    InputRowSchema rowSchema = new InputRowSchema(
        new TimestampSpec(null, null, null),
        DimensionsSpec.builder().useSchemaDiscovery(true).build(),
        null
    );

    InputSourceReader reader = inputSource.fixedFormatReader(
        rowSchema,
        null
    );
    CloseableIterator<InputRow> iterator = reader.read();

    InputRow first = iterator.next();
    InputRow generatorFirst = MapInputRowParser.parse(rowSchema, generator.nextRaw(rowSchema.getTimestampSpec().getTimestampColumn()));
    Assert.assertEquals(generatorFirst, first);
    Assert.assertTrue(iterator.hasNext());
    int i;
    for (i = 1; iterator.hasNext(); i++) {
      iterator.next();
    }
    Assert.assertEquals(numRows, i);
  }

  @Test
  public void testSplits()
  {
    GeneratorInputSource inputSource = new GeneratorInputSource(
        "basic",
        null,
        1000,
        2,
        1024L,
        DateTimes.nowUtc().getMillis(),
        1000,
        1.0
    );

    Assert.assertEquals(2, inputSource.estimateNumSplits(null, null));
    Assert.assertFalse(inputSource.needsFormat());
    Assert.assertEquals(2, inputSource.createSplits(null, null).count());
    Assert.assertEquals(
        new Long(2048L),
        ((GeneratorInputSource) inputSource.withSplit(new InputSplit<>(2048L))).getSeed()
    );
  }

  @Test
  public void testGetTypes()
  {
    GeneratorInputSource inputSource = new GeneratorInputSource(
        "basic",
        null,
        1000,
        2,
        1024L,
        DateTimes.nowUtc().getMillis(),
        1000,
        1.0
    );

    Assert.assertEquals(ImmutableSet.of(IndexingServiceInputSourceModule.GENERATOR_SCHEME), inputSource.getTypes());
  }
}
