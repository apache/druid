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

package org.apache.druid.frame.processor.test;

import com.google.common.collect.Iterables;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;

import java.util.List;

public final class TestFrameProcessorUtils
{
  private TestFrameProcessorUtils()
  {
  }

  public static StorageAdapter toStorageAdapter(List<InputRow> inputRows)
  {
    final IncrementalIndex index = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema(
                0,
                new TimestampSpec("__time", "millis", null),
                Granularities.NONE,
                VirtualColumns.EMPTY,
                DimensionsSpec.builder().useSchemaDiscovery(true).build(),
                new AggregatorFactory[0],
                false
            )
        )
        .setMaxRowCount(1000)
        .build();

    try {
      for (InputRow inputRow : inputRows) {
        index.add(inputRow);
      }
    }
    catch (IndexSizeExceededException e) {
      throw new RuntimeException(e);
    }

    return new IncrementalIndexStorageAdapter(index);
  }

  public static Frame toFrame(List<InputRow> inputRows)
  {
    final StorageAdapter storageAdapter = toStorageAdapter(inputRows);
    return Iterables.getOnlyElement(FrameSequenceBuilder.fromAdapter(storageAdapter)
                                                        .frameType(FrameType.ROW_BASED)
                                                        .frames()
                                                        .toList());
  }
}
