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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Assert;
import org.junit.Test;

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
    final MaxSizeSplitHintSpec original = new MaxSizeSplitHintSpec(1024L);
    final byte[] bytes = mapper.writeValueAsBytes(original);
    final MaxSizeSplitHintSpec fromJson = (MaxSizeSplitHintSpec) mapper.readValue(bytes, SplitHintSpec.class);
    Assert.assertEquals(original, fromJson);
  }

  @Test
  public void testCreateWithNullReturningDefaultMaxSplitSize()
  {
    Assert.assertEquals(MaxSizeSplitHintSpec.DEFAULT_MAX_SPLIT_SIZE, new MaxSizeSplitHintSpec(null).getMaxSplitSize());
  }

  @Test
  public void testSplitSmallInputsGroupingIntoLargerSplits()
  {
    final int eachInputSize = 3;
    final MaxSizeSplitHintSpec splitHintSpec = new MaxSizeSplitHintSpec(10L);
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
    final MaxSizeSplitHintSpec splitHintSpec = new MaxSizeSplitHintSpec(10L);
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
  public void testSplitSkippingEmptyInputs()
  {
    final int nonEmptyInputSize = 3;
    final MaxSizeSplitHintSpec splitHintSpec = new MaxSizeSplitHintSpec(10L);
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
    EqualsVerifier.forClass(MaxSizeSplitHintSpec.class).withNonnullFields("maxSplitSize").usingGetClass().verify();
  }
}
