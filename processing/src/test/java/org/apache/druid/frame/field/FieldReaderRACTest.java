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

package org.apache.druid.frame.field;

import com.google.common.collect.ImmutableList;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.write.FrameWriterTestData;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.concrete.RowBasedFrameRowsAndColumnsTest;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class FieldReaderRACTest extends InitializedNullHandlingTest
{
  public static final List<FrameWriterTestData.Dataset<?>> DATASETS =
      ImmutableList.<FrameWriterTestData.Dataset<?>>builder()
                   .add(FrameWriterTestData.TEST_FLOATS)
                   .add(FrameWriterTestData.TEST_DOUBLES)
                   .add(FrameWriterTestData.TEST_LONGS)
                   .add(FrameWriterTestData.TEST_STRINGS_SINGLE_VALUE)
                   // .add(FrameWriterTestData.TEST_STRINGS_MULTI_VALUE)
                   .add(FrameWriterTestData.TEST_ARRAYS_STRING)
                   .add(FrameWriterTestData.TEST_ARRAYS_LONG)
                   .add(FrameWriterTestData.TEST_ARRAYS_FLOAT)
                   .add(FrameWriterTestData.TEST_ARRAYS_DOUBLE)
                   .add(FrameWriterTestData.TEST_COMPLEX_HLL)
                   // .add(FrameWriterTestData.TEST_COMPLEX_NESTED)
                   .build();

  private final FrameWriterTestData.Dataset<?> dataset;

  @Parameterized.Parameters(name = "dataset = {0}")
  public static Collection<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    for (FrameWriterTestData.Dataset<?> dataset : DATASETS) {
      constructors.add(new Object[]{dataset});
    }

    return constructors;
  }

  public FieldReaderRACTest(FrameWriterTestData.Dataset<?> dataset)
  {
    this.dataset = dataset;
  }

  @Test
  public void testDataSet()
  {
    final ColumnType colType = dataset.getType();
    final List<Object> rows = (List) dataset.getData(KeyOrder.ASCENDING);

    final MapOfColumnsRowsAndColumns mapOfColumnsRowsAndColumns = MapOfColumnsRowsAndColumns.builder()
                                                                                            .add("dim1", rows.toArray(), colType)
                                                                                            .build();

    if (colType.isArray()) {
      Assert.assertThrows(
          DruidException.class,
          () -> {
            RowBasedFrameRowsAndColumnsTest.MAKER.apply(mapOfColumnsRowsAndColumns)
                                                 .findColumn("dim1")
                                                 .toAccessor();
          }
      );
    } else {
      final ColumnAccessor accessor = RowBasedFrameRowsAndColumnsTest.MAKER.apply(mapOfColumnsRowsAndColumns)
                                                      .findColumn("dim1")
                                                      .toAccessor();


      for (int i = 0; i < rows.size(); i++) {
        Assert.assertEquals(rows.get(i), accessor.getObject(i));
      }
    }
  }
}
