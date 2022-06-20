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

package org.apache.druid.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DirectQueryProcessingPool;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.DictionaryEncodedStringValueIndex;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;

public class DumpSegmentTest
{
  @Test
  public void testExecuteQuery()
  {
    Injector injector = Mockito.mock(Injector.class);
    QueryRunnerFactoryConglomerate conglomerate = Mockito.mock(QueryRunnerFactoryConglomerate.class);
    QueryRunnerFactory factory = Mockito.mock(QueryRunnerFactory.class, Mockito.RETURNS_DEEP_STUBS);
    QueryRunner runner = Mockito.mock(QueryRunner.class);
    QueryRunner mergeRunner = Mockito.mock(QueryRunner.class);
    Query query = Mockito.mock(Query.class);
    Sequence expected = Sequences.simple(Collections.singletonList(123));
    Mockito.when(injector.getInstance(QueryRunnerFactoryConglomerate.class)).thenReturn(conglomerate);
    Mockito.when(conglomerate.findFactory(ArgumentMatchers.any())).thenReturn(factory);
    Mockito.when(factory.createRunner(ArgumentMatchers.any())).thenReturn(runner);
    Mockito.when(factory.getToolchest().mergeResults(factory.mergeRunners(DirectQueryProcessingPool.INSTANCE, ImmutableList.of(runner)))).thenReturn(mergeRunner);
    Mockito.when(mergeRunner.run(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(expected);
    Sequence actual = DumpSegment.executeQuery(injector, null, query);
    Assert.assertSame(expected, actual);
  }

  @Test
  public void testDumpBitmap() throws IOException
  {
    Injector injector = Mockito.mock(Injector.class);
    QueryableIndex queryableIndex = Mockito.mock(QueryableIndex.class);
    ObjectMapper mapper = new DefaultObjectMapper();
    BitmapFactory bitmapFactory = new RoaringBitmapFactory();
    ColumnHolder xHolder = Mockito.mock(ColumnHolder.class);
    ColumnHolder yHolder = Mockito.mock(ColumnHolder.class);
    ColumnIndexSupplier indexSupplier = Mockito.mock(ColumnIndexSupplier.class);
    DictionaryEncodedStringValueIndex valueIndex = Mockito.mock(DictionaryEncodedStringValueIndex.class);

    ImmutableBitmap bitmap = bitmapFactory.complement(bitmapFactory.makeEmptyImmutableBitmap(), 10);
    Mockito.when(injector.getInstance(Key.get(ObjectMapper.class, Json.class))).thenReturn(mapper);

    Mockito.when(queryableIndex.getBitmapFactoryForDimensions()).thenReturn(bitmapFactory);

    Mockito.when(queryableIndex.getColumnHolder("x")).thenReturn(xHolder);
    Mockito.when(queryableIndex.getColumnHolder("y")).thenReturn(yHolder);

    Mockito.when(xHolder.getIndexSupplier()).thenReturn(indexSupplier);
    Mockito.when(indexSupplier.as(DictionaryEncodedStringValueIndex.class)).thenReturn(valueIndex);
    Mockito.when(valueIndex.getCardinality()).thenReturn(1);
    Mockito.when(valueIndex.getBitmap(0)).thenReturn(bitmap);
    Mockito.when(valueIndex.getValue(0)).thenReturn("val");

    DumpSegment.runBitmaps(
        injector,
        null,
        queryableIndex,
        ImmutableList.of("x", "y"),
        false
    );
    Assert.assertTrue(true);
  }
}
