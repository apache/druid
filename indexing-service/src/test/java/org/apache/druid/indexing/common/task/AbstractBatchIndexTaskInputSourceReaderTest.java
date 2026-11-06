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

import org.apache.druid.data.input.BatchInputSourceReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.LongArrayColumn;
import org.apache.druid.query.rowsandcols.column.ObjectArrayColumn;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.transform.TransformSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.Collections;

public class AbstractBatchIndexTaskInputSourceReaderTest
{
  @Test
  public void testUsesBatchReaderWithoutTransforms() throws Exception
  {
    final BatchInputSourceReader reader = Mockito.mock(BatchInputSourceReader.class);
    final InputSource inputSource = inputSource(reader);
    final RowsAndColumns batch = MapOfColumnsRowsAndColumns.builder()
                                                           .add("ts", new LongArrayColumn(new long[]{1_000L}))
                                                           .add(
                                                               "dim",
                                                               new ObjectArrayColumn(
                                                                   new Object[]{"value"},
                                                                   ColumnType.STRING
                                                               )
                                                           )
                                                           .build();
    Mockito.when(reader.readBatches(Mockito.any()))
           .thenReturn(CloseableIterators.withEmptyBaggage(Collections.singletonList(batch).iterator()));

    try (CloseableIterator<InputRow> rows = AbstractBatchIndexTask.inputSourceReader(
        new File("."),
        dataSchema(TransformSpec.NONE),
        inputSource,
        null,
        row -> true,
        meters(),
        parseExceptionHandler()
    )) {
      Assertions.assertTrue(rows.hasNext());
      Assertions.assertEquals("value", rows.next().getRaw("dim"));
      Assertions.assertFalse(rows.hasNext());
    }

    Mockito.verify(reader).readBatches(Mockito.any());
    Mockito.verify(reader, Mockito.never()).read(Mockito.any());
  }

  @Test
  public void testUsesRowReaderWithTransforms() throws Exception
  {
    final BatchInputSourceReader reader = Mockito.mock(BatchInputSourceReader.class);
    final InputSource inputSource = inputSource(reader);
    Mockito.when(reader.read(Mockito.any()))
           .thenReturn(CloseableIterators.withEmptyBaggage(Collections.emptyIterator()));
    final TransformSpec transformSpec = new TransformSpec(
        new SelectorDimFilter("dim", "value", null),
        null
    );

    try (CloseableIterator<InputRow> ignored = AbstractBatchIndexTask.inputSourceReader(
        new File("."),
        dataSchema(transformSpec),
        inputSource,
        null,
        row -> true,
        meters(),
        parseExceptionHandler()
    )) {
      Assertions.assertFalse(ignored.hasNext());
    }

    Mockito.verify(reader).read(Mockito.any());
    Mockito.verify(reader, Mockito.never()).readBatches(Mockito.any());
  }

  private static InputSource inputSource(final BatchInputSourceReader reader)
  {
    final InputSource inputSource = Mockito.mock(InputSource.class);
    Mockito.when(inputSource.reader(Mockito.any(), Mockito.isNull(), Mockito.any())).thenReturn(reader);
    return inputSource;
  }

  private static DataSchema dataSchema(final TransformSpec transformSpec)
  {
    return DataSchema.builder()
                     .withDataSource("test")
                     .withTimestamp(new TimestampSpec("ts", "millis", null))
                     .withDimensions(new StringDimensionSchema("dim"))
                     .withTransform(transformSpec)
                     .build();
  }

  private static RowIngestionMeters meters()
  {
    return new SimpleRowIngestionMeters();
  }

  private static ParseExceptionHandler parseExceptionHandler()
  {
    return new ParseExceptionHandler(meters(), false, 0, 0);
  }
}
