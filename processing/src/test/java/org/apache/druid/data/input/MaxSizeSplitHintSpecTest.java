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

package org.apache.druid.data.input;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

public class MaxSizeSplitHintSpecTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final MaxSizeSplitHintSpec original = new MaxSizeSplitHintSpec(new HumanReadableBytes(1024L), 20_000);
    final byte[] bytes = mapper.writeValueAsBytes(original);
    final MaxSizeSplitHintSpec fromJson = (MaxSizeSplitHintSpec) mapper.readValue(bytes, SplitHintSpec.class);
    Assertions.assertEquals(original, fromJson);
  }

  @Test
  public void testReadFromJson() throws JsonProcessingException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final String json = "{"
                        + "  \"type\":\"maxSize\","
                        + "  \"maxSplitSize\":1024,"
                        + "  \"maxNumFiles\":20000"
                        + "}\n";
    final MaxSizeSplitHintSpec fromJson = (MaxSizeSplitHintSpec) mapper.readValue(json, SplitHintSpec.class);
    Assertions.assertEquals(new MaxSizeSplitHintSpec(new HumanReadableBytes(1024L), 20_000), fromJson);
  }

  @Test
  public void testConstructorWith0MaxNumFiles()
  {
    IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new MaxSizeSplitHintSpec(null, 0)
    );
    Assertions.assertTrue(e.getMessage().contains("maxNumFiles should be larger than 0"));
  }

  @Test
  public void testConstructorWith0MaxSplitSize()
  {
    IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new MaxSizeSplitHintSpec(0, null)
    );
    Assertions.assertTrue(e.getMessage().contains("maxSplitSize should be larger than 0"));
  }

  @Test
  public void testDefaults()
  {
    Assertions.assertEquals(
        MaxSizeSplitHintSpec.DEFAULT_MAX_SPLIT_SIZE,
        new MaxSizeSplitHintSpec(null, null).getMaxSplitSize()
    );
    Assertions.assertEquals(
        MaxSizeSplitHintSpec.DEFAULT_MAX_NUM_FILES,
        new MaxSizeSplitHintSpec(null, null).getMaxNumFiles()
    );
  }

  @Test
  public void testSplitSmallInputsGroupingIntoLargerSplits()
  {
    final int eachInputSize = 3;
    final MaxSizeSplitHintSpec splitHintSpec = new MaxSizeSplitHintSpec(new HumanReadableBytes(10L), 10_000);
    final Function<Integer, InputFileAttribute> inputAttributeExtractor = InputFileAttribute::new;
    final List<List<Integer>> splits = Lists.newArrayList(
        splitHintSpec.split(IntStream.generate(() -> eachInputSize).limit(10).iterator(), inputAttributeExtractor)
    );
    Assertions.assertEquals(4, splits.size());
    Assertions.assertEquals(3, splits.get(0).size());
    Assertions.assertEquals(3, splits.get(1).size());
    Assertions.assertEquals(3, splits.get(2).size());
    Assertions.assertEquals(1, splits.get(3).size());
  }

  @Test
  public void testSplitLargeInputsReturningSplitsOfSingleInput()
  {
    final int eachInputSize = 15;
    final MaxSizeSplitHintSpec splitHintSpec = new MaxSizeSplitHintSpec(new HumanReadableBytes(10L), 10_000);
    final Function<Integer, InputFileAttribute> inputAttributeExtractor = InputFileAttribute::new;
    final List<List<Integer>> splits = Lists.newArrayList(
        splitHintSpec.split(IntStream.generate(() -> eachInputSize).limit(10).iterator(), inputAttributeExtractor)
    );
    Assertions.assertEquals(10, splits.size());
    for (List<Integer> split : splits) {
      Assertions.assertEquals(1, split.size());
    }
  }

  @Test
  public void testSplitSmallInputsWithMaxNumFilesEachSplitShouldHaveLessFilesAssigned()
  {
    final int eachInputSize = 3;
    final MaxSizeSplitHintSpec splitHintSpec = new MaxSizeSplitHintSpec(new HumanReadableBytes("500M"), 3);
    final Function<Integer, InputFileAttribute> inputAttributeExtractor = InputFileAttribute::new;
    final List<List<Integer>> splits = Lists.newArrayList(
        splitHintSpec.split(IntStream.generate(() -> eachInputSize).limit(10).iterator(), inputAttributeExtractor)
    );
    Assertions.assertEquals(4, splits.size());
    Assertions.assertEquals(3, splits.get(0).size());
    Assertions.assertEquals(3, splits.get(1).size());
    Assertions.assertEquals(3, splits.get(2).size());
    Assertions.assertEquals(1, splits.get(3).size());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(MaxSizeSplitHintSpec.class)
                  .withNonnullFields("maxSplitSize")
                  .withNonnullFields("maxNumFiles")
                  .usingGetClass()
                  .verify();
  }
}
