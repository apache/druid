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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.com.google.common.collect.Lists;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

public class MaxSizeSplitHintSpecTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final MaxSizeSplitHintSpec original = new MaxSizeSplitHintSpec(new HumanReadableBytes(1024L), 20_000);
    final byte[] bytes = mapper.writeValueAsBytes(original);
    final MaxSizeSplitHintSpec fromJson = (MaxSizeSplitHintSpec) mapper.readValue(bytes, SplitHintSpec.class);
    Assert.assertEquals(original, fromJson);
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
    Assert.assertEquals(new MaxSizeSplitHintSpec(new HumanReadableBytes(1024L), 20_000), fromJson);
  }

  @Test
  public void testConstructorWith0MaxNumFiles()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("maxNumFiles should be larger than 0");
    new MaxSizeSplitHintSpec(null, 0);
  }

  @Test
  public void testConstructorWith0MaxSplitSize()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("maxSplitSize should be larger than 0");
    new MaxSizeSplitHintSpec(0, null);
  }

  @Test
  public void testDefaults()
  {
    Assert.assertEquals(
        MaxSizeSplitHintSpec.DEFAULT_MAX_SPLIT_SIZE,
        new MaxSizeSplitHintSpec(null, null).getMaxSplitSize()
    );
    Assert.assertEquals(
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
    Assert.assertEquals(4, splits.size());
    Assert.assertEquals(3, splits.get(0).size());
    Assert.assertEquals(3, splits.get(1).size());
    Assert.assertEquals(3, splits.get(2).size());
    Assert.assertEquals(1, splits.get(3).size());
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
    Assert.assertEquals(10, splits.size());
    for (List<Integer> split : splits) {
      Assert.assertEquals(1, split.size());
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
    Assert.assertEquals(4, splits.size());
    Assert.assertEquals(3, splits.get(0).size());
    Assert.assertEquals(3, splits.get(1).size());
    Assert.assertEquals(3, splits.get(2).size());
    Assert.assertEquals(1, splits.get(3).size());
  }

  @Test
  public void testSplitSkippingEmptyInputs()
  {
    final int nonEmptyInputSize = 3;
    final MaxSizeSplitHintSpec splitHintSpec = new MaxSizeSplitHintSpec(new HumanReadableBytes(10L), null);
    final Function<Integer, InputFileAttribute> inputAttributeExtractor = InputFileAttribute::new;
    final IntStream dataStream = IntStream.concat(
        IntStream.concat(
            IntStream.generate(() -> 0).limit(10),
            IntStream.generate(() -> nonEmptyInputSize).limit(10)
        ),
        IntStream.generate(() -> 0).limit(10)
    );
    final List<List<Integer>> splits = Lists.newArrayList(
        splitHintSpec.split(dataStream.iterator(), inputAttributeExtractor)
    );
    Assert.assertEquals(4, splits.size());
    Assert.assertEquals(3, splits.get(0).size());
    Assert.assertEquals(3, splits.get(1).size());
    Assert.assertEquals(3, splits.get(2).size());
    Assert.assertEquals(1, splits.get(3).size());
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
