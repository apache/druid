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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@SuppressWarnings({"NullableProblems", "ConstantConditions"})
public class InlineFirehoseFactoryTest
{
  private static final String DIMENSION_0 = "timestamp";
  private static final String DIMENSION_1 = "value";
  private static final List<String> DIMENSIONS = Arrays.asList(DIMENSION_0, DIMENSION_1);
  private static final String DELIMITER = ",";
  private static final StringInputRowParser PARSER = new StringInputRowParser(
      new CSVParseSpec(
          new TimestampSpec(
              DIMENSION_0,
              "auto",
              null
          ),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(DIMENSIONS),
              Collections.emptyList(),
              Collections.emptyList()
          ),
          DELIMITER,
          DIMENSIONS,
          false,
          0
      ),
      StandardCharsets.UTF_8.name()
  );
  private static final File NO_TEMP_DIR = null;
  private static final String TIMESTAMP = "0";
  private static final String VALUE = "a";
  private static final String DATA = TIMESTAMP + DELIMITER + VALUE;

  private InlineFirehoseFactory target;

  @Before
  public void setUp()
  {
    target = new InlineFirehoseFactory(DATA);
  }

  @Test
  public void testInterfaceImplementation()
  {
    Assert.assertTrue(target instanceof FiniteFirehoseFactory);
    Assert.assertFalse(target.isSplittable());
    Assert.assertEquals(1, target.getNumSplits(null));
  }

  @Test(expected = NullPointerException.class)
  public void testContstructorDataRequired()
  {
    new InlineFirehoseFactory(null);
  }

  @Test
  public void testGetData()
  {
    Assert.assertEquals(DATA, target.getData());
  }

  @Test
  public void testConnect() throws IOException
  {
    Firehose firehose = target.connect(PARSER, NO_TEMP_DIR);
    InputRow row = firehose.nextRow();
    Assert.assertNotNull(row);
    List<String> values = row.getDimension(DIMENSION_1);
    Assert.assertNotNull(values);
    Assert.assertEquals(1, values.size());
    Assert.assertEquals(VALUE, values.get(0));
  }

  @Test
  public void testForcedSplitAndClone()
  {
    Optional<InputSplit<String>> inputSplitOptional = target.getSplits(null).findFirst();
    Assert.assertTrue(inputSplitOptional.isPresent());
    FiniteFirehoseFactory<StringInputRowParser, String> cloneWithSplit = target.withSplit(inputSplitOptional.get());
    Assert.assertTrue(cloneWithSplit instanceof InlineFirehoseFactory);
    Assert.assertEquals(DATA, ((InlineFirehoseFactory) cloneWithSplit).getData());
  }

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    InlineFirehoseFactory factory = new InlineFirehoseFactory(DATA);
    String serialized = objectMapper.writeValueAsString(factory);
    InlineFirehoseFactory deserialized = objectMapper.readValue(serialized, InlineFirehoseFactory.class);
    Assert.assertEquals(factory, deserialized);
  }
}
