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

package org.apache.druid.indextable.loader;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.join.table.RowBasedIndexedTable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;


@RunWith(MockitoJUnitRunner.class)
public class IndexedTableSupplierTest
{
  private static final List<String> INDEX_KEYS = ImmutableList.of("key1", "key2");

  private static final List<DimensionSchema> COLUMN_SCHEMA = ImmutableList.of();

  private StringDimensionSchema key1DimentsionSchema;
  private StringDimensionSchema key2DimentsionSchema;
  private DoubleDimensionSchema metric1DimensionSchema;

  @Mock(answer = Answers.RETURNS_MOCKS)
  private IndexTask.IndexIngestionSpec ingestionSpec;
  @Mock(answer = Answers.RETURNS_MOCKS)
  private DataSchema dataSchema;
  @Mock(answer = Answers.RETURNS_MOCKS)
  private DimensionsSpec dimensionsSpec;
  @Mock
  private IndexTask.IndexIOConfig indexIOConfig;
  @Mock
  private InputSourceReader inputSourceReader;
  @Mock
  private File tmpDir;
  @Mock
  private InputRow row1;
  @Mock
  private InputRow row2;
  @Mock
  private Closeable closeable;
  private CloseableIterator<InputRow> rowIterator;

  private IndexedTableSupplier target;

  @Before
  public void setUp() throws IOException
  {
    key1DimentsionSchema = new StringDimensionSchema("key1");
    key2DimentsionSchema = new StringDimensionSchema("key2");
    metric1DimensionSchema = new DoubleDimensionSchema("metric1");

    Mockito.when(ingestionSpec.getDataSchema()).thenReturn(dataSchema);
    Mockito.when(dataSchema.getDimensionsSpec()).thenReturn(dimensionsSpec);
    Mockito.when(dimensionsSpec.getDimensions())
           .thenReturn(ImmutableList.of(key1DimentsionSchema, key2DimentsionSchema, metric1DimensionSchema));
    rowIterator = CloseableIterators.wrap(ImmutableList.of(row1, row2).iterator(), closeable);
    Mockito.when(inputSourceReader.read()).thenReturn(rowIterator);

    target = Mockito.spy(new IndexedTableSupplier(INDEX_KEYS, ingestionSpec, () -> tmpDir));
    Mockito.doReturn(inputSourceReader).when(target).getInputSourceReader(dataSchema, tmpDir);
  }

  @Test
  public void testGetShouldReturnARowBasedIndexedTableAndExhaustIterator() throws IOException
  {
    IndexedTable table = target.get();
    Assert.assertEquals(2, table.numRows());
    Assert.assertEquals(RowBasedIndexedTable.class, table.getClass());
    Assert.assertFalse(rowIterator.hasNext());
  }

  @Test(expected = ISE.class)
  public void testGetWithKeysMissingInIngestionSpecShouldThrowISE()
  {
    Mockito.doReturn(ImmutableList.of(key1DimentsionSchema, metric1DimensionSchema))
           .when(dimensionsSpec).getDimensions();
    target.get();
  }
}
