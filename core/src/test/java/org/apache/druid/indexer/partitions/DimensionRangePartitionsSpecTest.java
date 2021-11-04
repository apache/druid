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

package org.apache.druid.indexer.partitions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DimensionRangePartitionsSpecTest
{
  private static final Integer TARGET_ROWS_PER_SEGMENT = 1;
  private static final Integer MAX_ROWS_PER_SEGMENT = null;
  private static final Integer HISTORICAL_NULL = PartitionsSpec.HISTORICAL_NULL;
  private static final List<String> PARTITION_DIMENSIONS = Arrays.asList("a", "b");
  private static final boolean ASSUME_GROUPED = false;
  private static final DimensionRangePartitionsSpec SPEC = new DimensionRangePartitionsSpec(
      TARGET_ROWS_PER_SEGMENT,
      MAX_ROWS_PER_SEGMENT,
      PARTITION_DIMENSIONS,
      ASSUME_GROUPED
  );
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void serde()
  {
    String json = serialize(SPEC);
    DimensionRangePartitionsSpec spec = deserialize(json);
    Assert.assertEquals(SPEC, spec);
  }

  @Test
  public void havingNeitherTargetNorMaxForbidden()
  {
    new TestSpecBuilder()
        .testIllegalArgumentException("Exactly one of targetRowsPerSegment or maxRowsPerSegment must be present");
  }

  @Test
  public void targetRowsPerSegmentMustBePositive()
  {
    new TestSpecBuilder()
        .targetRowsPerSegment(0)
        .testIllegalArgumentException("targetRowsPerSegment must be greater than 0");
  }

  @Test
  public void targetRowsPerSegmentHistoricalNull()
  {
    new TestSpecBuilder()
        .targetRowsPerSegment(HISTORICAL_NULL)
        .testIllegalArgumentException("Exactly one of targetRowsPerSegment or maxRowsPerSegment must be present");
  }

  @Test
  public void targetMaxRowsPerSegmentOverflows()
  {
    new TestSpecBuilder()
        .targetRowsPerSegment(Integer.MAX_VALUE)
        .testIllegalArgumentException("targetRowsPerSegment is too large");
  }

  @Test
  public void maxRowsPerSegmentMustBePositive()
  {
    new TestSpecBuilder()
        .maxRowsPerSegment(0)
        .testIllegalArgumentException("maxRowsPerSegment must be greater than 0");
  }

  @Test
  public void maxRowsPerSegmentHistoricalNull()
  {
    new TestSpecBuilder()
        .maxRowsPerSegment(HISTORICAL_NULL)
        .testIllegalArgumentException("Exactly one of targetRowsPerSegment or maxRowsPerSegment must be present");
  }

  @Test
  public void resolvesMaxFromTargetRowsPerSegment()
  {
    DimensionRangePartitionsSpec spec = new TestSpecBuilder()
        .targetRowsPerSegment(123)
        .build();
    Assert.assertEquals(184, spec.getMaxRowsPerSegment().intValue());
  }

  @Test
  public void resolvesMaxFromMaxRowsPerSegment()
  {
    DimensionRangePartitionsSpec spec = new TestSpecBuilder()
        .maxRowsPerSegment(123)
        .build();
    Assert.assertEquals(123, spec.getMaxRowsPerSegment().intValue());
  }

  @Test
  public void getPartitionDimensionFromNull()
  {
    // Verify that partitionDimensions must be non-null
    new TestSpecBuilder()
        .partitionDimensions(null)
        .testIllegalArgumentException("partitionDimensions must be specified");
  }

  @Test
  public void getPartitionDimensionFromNonNull()
  {
    List<String> partitionDimensions = Collections.singletonList("a");
    DimensionRangePartitionsSpec spec = new TestSpecBuilder()
        .targetRowsPerSegment(10)
        .partitionDimensions(partitionDimensions)
        .build();
    Assert.assertEquals(partitionDimensions, spec.getPartitionDimensions());
  }

  private static String serialize(Object object)
  {
    try {
      return OBJECT_MAPPER.writeValueAsString(object);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static DimensionRangePartitionsSpec deserialize(String serialized)
  {
    try {
      return OBJECT_MAPPER.readValue(serialized, DimensionRangePartitionsSpec.class);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Spec builder used in this test.
   */
  private class TestSpecBuilder
  {
    private Integer targetRowsPerSegment;
    private Integer maxRowsPerSegment;
    private List<String> partitionDimensions = Collections.emptyList();

    TestSpecBuilder targetRowsPerSegment(Integer targetRowsPerSegment)
    {
      this.targetRowsPerSegment = targetRowsPerSegment;
      return this;
    }

    TestSpecBuilder maxRowsPerSegment(Integer maxRowsPerSegment)
    {
      this.maxRowsPerSegment = maxRowsPerSegment;
      return this;
    }

    TestSpecBuilder partitionDimensions(List<String> partitionDimensions)
    {
      this.partitionDimensions = partitionDimensions;
      return this;
    }

    void testIllegalArgumentException(String exceptionExpectedMessage)
    {
      exception.expect(IllegalArgumentException.class);
      exception.expectMessage(exceptionExpectedMessage);
      build();
    }

    DimensionRangePartitionsSpec build()
    {
      return new DimensionRangePartitionsSpec(
          targetRowsPerSegment,
          maxRowsPerSegment,
          partitionDimensions,
          ASSUME_GROUPED
      );
    }
  }
}
