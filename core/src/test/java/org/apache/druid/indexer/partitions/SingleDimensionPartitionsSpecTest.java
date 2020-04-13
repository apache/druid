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

import java.util.Collections;

public class SingleDimensionPartitionsSpecTest
{
  private static final Integer TARGET_ROWS_PER_SEGMENT = 1;
  private static final Integer MAX_ROWS_PER_SEGMENT = null;
  private static final Integer HISTORICAL_NULL = PartitionsSpec.HISTORICAL_NULL;
  private static final String PARTITION_DIMENSION = "a";
  private static final boolean ASSUME_GROUPED = false;
  private static final SingleDimensionPartitionsSpec SPEC = new SingleDimensionPartitionsSpec(
      TARGET_ROWS_PER_SEGMENT,
      MAX_ROWS_PER_SEGMENT,
      PARTITION_DIMENSION,
      ASSUME_GROUPED
  );
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void serde()
  {
    String json = serialize(SPEC);
    SingleDimensionPartitionsSpec spec = deserialize(json);
    Assert.assertEquals(SPEC, spec);
  }

  @Test
  public void deserializeWithBackwardCompatibility()
  {
    String serialized = "{"
                        + "\"type\":\"" + SingleDimensionPartitionsSpec.NAME + "\""
                        + ",\"targetPartitionSize\":" + TARGET_ROWS_PER_SEGMENT  // test backward-compatible for this
                        + ",\"maxPartitionSize\":" + MAX_ROWS_PER_SEGMENT  // test backward-compatible for this
                        + ",\"partitionDimension\":\"" + PARTITION_DIMENSION + "\""
                        + ",\"assumeGrouped\":" + ASSUME_GROUPED
                        + "}";
    SingleDimensionPartitionsSpec spec = deserialize(serialized);
    Assert.assertEquals(SPEC, spec);
  }

  @Test
  public void havingBothTargetForbidden()
  {
    new Tester()
        .targetRowsPerSegment(1)
        .targetPartitionSize(1)
        .testIllegalArgumentException("At most one of [Property{name='targetRowsPerSegment', value=1}] or [Property{name='targetPartitionSize', value=1}] must be present");
  }

  @Test
  public void havingBothMaxForbidden()
  {
    new Tester()
        .maxRowsPerSegment(1)
        .maxPartitionSize(1)
        .testIllegalArgumentException("At most one of [Property{name='maxRowsPerSegment', value=1}] or [Property{name='maxPartitionSize', value=1}] must be present");
  }

  @Test
  public void havingNeitherTargetNorMaxForbidden()
  {
    new Tester()
        .testIllegalArgumentException("Exactly one of targetRowsPerSegment or maxRowsPerSegment must be present");
  }

  @Test
  public void targetRowsPerSegmentMustBePositive()
  {
    new Tester()
        .targetRowsPerSegment(0)
        .testIllegalArgumentException("targetRowsPerSegment must be greater than 0");
  }

  @Test
  public void targetRowsPerSegmentHistoricalNull()
  {
    new Tester()
        .targetRowsPerSegment(HISTORICAL_NULL)
        .testIllegalArgumentException("Exactly one of targetRowsPerSegment or maxRowsPerSegment must be present");
  }

  @Test
  public void targetPartitionSizeMustBePositive()
  {
    new Tester()
        .targetPartitionSize(0)
        .testIllegalArgumentException("targetPartitionSize must be greater than 0");
  }

  @Test
  public void targetMaxRowsPerSegmentOverflows()
  {
    new Tester()
        .targetRowsPerSegment(Integer.MAX_VALUE)
        .testIllegalArgumentException("targetRowsPerSegment is too large");
  }

  @Test
  public void targetPartitionSizeOverflows()
  {
    new Tester()
        .targetPartitionSize(Integer.MAX_VALUE)
        .testIllegalArgumentException("targetPartitionSize is too large");
  }

  @Test
  public void maxRowsPerSegmentMustBePositive()
  {
    new Tester()
        .maxRowsPerSegment(0)
        .testIllegalArgumentException("maxRowsPerSegment must be greater than 0");
  }

  @Test
  public void maxRowsPerSegmentHistoricalNull()
  {
    new Tester()
        .maxRowsPerSegment(HISTORICAL_NULL)
        .testIllegalArgumentException("Exactly one of targetRowsPerSegment or maxRowsPerSegment must be present");
  }

  @Test
  public void maxPartitionSizeMustBePositive()
  {
    new Tester()
        .maxPartitionSize(0)
        .testIllegalArgumentException("maxPartitionSize must be greater than 0");
  }

  @Test
  public void maxPartitionHistoricalNull()
  {
    new Tester()
        .maxPartitionSize(HISTORICAL_NULL)
        .testIllegalArgumentException("Exactly one of targetRowsPerSegment or maxRowsPerSegment must be present");
  }

  @Test
  public void resolvesMaxFromTargetRowsPerSegment()
  {
    SingleDimensionPartitionsSpec spec = new Tester()
        .targetRowsPerSegment(123)
        .build();
    Assert.assertEquals(184, spec.getMaxRowsPerSegment().intValue());
  }

  @Test
  public void resolvesMaxFromTargetPartitionSize()
  {
    SingleDimensionPartitionsSpec spec = new Tester()
        .targetPartitionSize(123)
        .build();
    Assert.assertEquals(Integer.valueOf(184), spec.getMaxRowsPerSegment());
  }

  @Test
  public void resolvesMaxFromMaxRowsPerSegment()
  {
    SingleDimensionPartitionsSpec spec = new Tester()
        .maxRowsPerSegment(123)
        .build();
    Assert.assertEquals(123, spec.getMaxRowsPerSegment().intValue());
  }

  @Test
  public void resolvesMaxFromMaxPartitionSize()
  {
    SingleDimensionPartitionsSpec spec = new Tester()
        .maxPartitionSize(123)
        .build();
    Assert.assertEquals(123, spec.getMaxRowsPerSegment().intValue());
  }

  @Test
  public void getPartitionDimensionFromNull()
  {
    SingleDimensionPartitionsSpec spec = new Tester()
        .targetPartitionSize(1)
        .partitionDimension(null)
        .build();
    Assert.assertEquals(Collections.emptyList(), spec.getPartitionDimensions());
  }

  @Test
  public void getPartitionDimensionFromNonNull()
  {
    String partitionDimension = "a";
    SingleDimensionPartitionsSpec spec = new Tester()
        .targetPartitionSize(1)
        .partitionDimension(partitionDimension)
        .build();
    Assert.assertEquals(Collections.singletonList(partitionDimension), spec.getPartitionDimensions());

  }

  private static String serialize(SingleDimensionPartitionsSpec spec)
  {
    try {
      return OBJECT_MAPPER.writeValueAsString(spec);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static SingleDimensionPartitionsSpec deserialize(String serialized)
  {
    try {
      return OBJECT_MAPPER.readValue(serialized, SingleDimensionPartitionsSpec.class);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private class Tester
  {
    private Integer targetRowsPerSegment;
    private Integer maxRowsPerSegment;
    private String partitionDimension;
    private Integer targetPartitionSize;
    private Integer maxPartitionSize;

    Tester targetRowsPerSegment(Integer targetRowsPerSegment)
    {
      this.targetRowsPerSegment = targetRowsPerSegment;
      return this;
    }

    Tester maxRowsPerSegment(Integer maxRowsPerSegment)
    {
      this.maxRowsPerSegment = maxRowsPerSegment;
      return this;
    }

    Tester partitionDimension(String partitionDimension)
    {
      this.partitionDimension = partitionDimension;
      return this;
    }

    Tester targetPartitionSize(Integer targetPartitionSize)
    {
      this.targetPartitionSize = targetPartitionSize;
      return this;
    }

    Tester maxPartitionSize(Integer maxPartitionSize)
    {
      this.maxPartitionSize = maxPartitionSize;
      return this;
    }

    void testIllegalArgumentException(String exceptionExpectedMessage)
    {
      exception.expect(IllegalArgumentException.class);
      exception.expectMessage(exceptionExpectedMessage);
      build();
    }

    SingleDimensionPartitionsSpec build()
    {
      return new SingleDimensionPartitionsSpec(
          targetRowsPerSegment,
          maxRowsPerSegment,
          partitionDimension,
          SingleDimensionPartitionsSpecTest.ASSUME_GROUPED,
          targetPartitionSize,
          maxPartitionSize
      );
    }
  }
}
