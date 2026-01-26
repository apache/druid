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

package org.apache.druid.indexing.common.task;

import com.google.common.collect.ImmutableList;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;

public class NativeCompactionRunnerTest
{
  private static final NativeCompactionRunner NATIVE_COMPACTION_RUNNER = new NativeCompactionRunner(
      Mockito.mock(SegmentCacheManagerFactory.class)
  );

  @Test
  public void testVirtualColumnsInTransformSpecAreNotSupported()
  {
    VirtualColumns virtualColumns = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn(
                "extractedField",
                "json_value(metadata, '$.category')",
                ColumnType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
    );

    CompactionTransformSpec transformSpec = new CompactionTransformSpec(null, virtualColumns);

    CompactionTask compactionTask = createCompactionTask(transformSpec);
    Map<Interval, DataSchema> intervalDataschemas = Collections.emptyMap();

    CompactionConfigValidationResult validationResult = NATIVE_COMPACTION_RUNNER.validateCompactionTask(
        compactionTask,
        intervalDataschemas
    );

    Assert.assertFalse(validationResult.isValid());
    Assert.assertEquals(
        "Virtual columns in filter rules are not supported by the Native compaction engine. Use MSQ compaction engine instead.",
        validationResult.getReason()
    );
  }

  @Test
  public void testNoVirtualColumnsIsValid()
  {
    CompactionTask compactionTask = createCompactionTask(null);
    Map<Interval, DataSchema> intervalDataschemas = Collections.emptyMap();

    CompactionConfigValidationResult validationResult = NATIVE_COMPACTION_RUNNER.validateCompactionTask(
        compactionTask,
        intervalDataschemas
    );

    Assert.assertTrue(validationResult.isValid());
  }

  @Test
  public void testEmptyVirtualColumnsIsValid()
  {
    CompactionTransformSpec transformSpec = new CompactionTransformSpec(null, VirtualColumns.EMPTY);

    CompactionTask compactionTask = createCompactionTask(transformSpec);
    Map<Interval, DataSchema> intervalDataschemas = Collections.emptyMap();

    CompactionConfigValidationResult validationResult = NATIVE_COMPACTION_RUNNER.validateCompactionTask(
        compactionTask,
        intervalDataschemas
    );

    Assert.assertTrue(validationResult.isValid());
  }

  private CompactionTask createCompactionTask(CompactionTransformSpec transformSpec)
  {
    SegmentCacheManagerFactory segmentCacheManagerFactory = Mockito.mock(SegmentCacheManagerFactory.class);
    CompactionTask.Builder builder = new CompactionTask.Builder(
        "dataSource",
        segmentCacheManagerFactory
    );
    builder.inputSpec(new CompactionIntervalSpec(Intervals.of("2020-01-01/2020-01-02"), null));
    builder.transformSpec(transformSpec);
    return builder.build();
  }
}
