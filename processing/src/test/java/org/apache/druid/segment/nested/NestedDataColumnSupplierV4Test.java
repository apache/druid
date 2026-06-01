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

package org.apache.druid.segment.nested;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.semantic.StringValueSetIndexes;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.utils.CompressionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class NestedDataColumnSupplierV4Test extends InitializedNullHandlingTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();
  private static final String NO_MATCH = "no";
  @TempDir
  private Path tempDir;
  DefaultBitmapResultFactory resultFactory = new DefaultBitmapResultFactory(new RoaringBitmapFactory());
  List<Map<String, Object>> data = ImmutableList.of(
      TestHelper.makeMap("x", 1L, "y", 1.0, "z", "a", "v", "100", "nullish", "notnull"),
      TestHelper.makeMap("y", 3.0, "z", "d", "v", 1000L, "nullish", null),
      TestHelper.makeMap("x", 5L, "y", 5.0, "z", "b", "nullish", ""),
      TestHelper.makeMap("x", 3L, "y", 4.0, "z", "c", "v", 3000.333, "nullish", "null"),
      TestHelper.makeMap("x", 2L, "v", "40000"),
      TestHelper.makeMap("x", 4L, "y", 2.0, "z", "e", "v", 11111L, "nullish", null)
  );

  Closer closer = Closer.create();

  @BeforeAll
  public static void staticSetup()
  {
    BuiltInTypesModule.registerHandlersAndSerde();
  }

  @AfterEach
  public void teardown() throws IOException
  {
    closer.close();
  }

  @Test
  public void testLegacyV3ReaderFormat() throws IOException
  {
    String columnName = "shipTo";
    String firstValue = "Cole";
    File tmpLocation = FileUtils.createTempDirInLocation(tempDir, "v3");
    CompressionUtils.unzip(
        NestedDataColumnSupplierV4Test.class.getClassLoader().getResourceAsStream("nested_segment_v3/index.zip"),
        tmpLocation
    );
    try (Closer closer = Closer.create()) {
      QueryableIndex theIndex = closer.register(TestHelper.getTestIndexIO().loadIndex(tmpLocation));
      ColumnHolder holder = theIndex.getColumnHolder(columnName);
      Assertions.assertNotNull(holder);
      Assertions.assertEquals(ColumnType.NESTED_DATA, holder.getCapabilities().toColumnType());
      NestedDataColumnV3<?, ?> v3 = closer.register((NestedDataColumnV3<?, ?>) holder.getColumn());
      Assertions.assertNotNull(v3);
      List<NestedPathPart> path = ImmutableList.of(new NestedPathField("lastName"));
      ColumnHolder nestedColumnHolder = v3.getColumnHolder(path);
      Assertions.assertNotNull(nestedColumnHolder);
      Assertions.assertEquals(ColumnType.STRING, nestedColumnHolder.getCapabilities().toColumnType());
      NestedFieldDictionaryEncodedColumn<?> nestedColumn =
          (NestedFieldDictionaryEncodedColumn<?>) nestedColumnHolder.getColumn();
      Assertions.assertNotNull(nestedColumn);
      ColumnValueSelector<?> selector = nestedColumn.makeColumnValueSelector(
          new SimpleAscendingOffset(theIndex.getNumRows())
      );
      ColumnIndexSupplier indexSupplier = v3.getColumnIndexSupplier(path);
      Assertions.assertNotNull(indexSupplier);
      StringValueSetIndexes valueSetIndex = indexSupplier.as(StringValueSetIndexes.class);
      Assertions.assertNotNull(valueSetIndex);
      BitmapColumnIndex indexForValue = valueSetIndex.forValue(firstValue);
      Assertions.assertEquals(firstValue, selector.getObject());
      Assertions.assertTrue(indexForValue.computeBitmapResult(resultFactory, false).get(0));
    }
  }

  @Test
  public void testLegacyV4ReaderFormat() throws IOException
  {
    String columnName = "shipTo";
    // i accidentally didn't use same segment granularity for v3 and v4 segments... so they have different first value
    String firstValue = "Beatty";
    File tmpLocation = FileUtils.createTempDirInLocation(tempDir, "v4");
    CompressionUtils.unzip(
        NestedDataColumnSupplierV4Test.class.getClassLoader().getResourceAsStream("nested_segment_v4/index.zip"),
        tmpLocation
    );
    try (Closer closer = Closer.create()) {
      QueryableIndex theIndex = closer.register(TestHelper.getTestIndexIO().loadIndex(tmpLocation));
      ColumnHolder holder = theIndex.getColumnHolder(columnName);
      Assertions.assertNotNull(holder);
      Assertions.assertEquals(ColumnType.NESTED_DATA, holder.getCapabilities().toColumnType());
      NestedDataColumnV4<?, ?> v4 = closer.register((NestedDataColumnV4<?, ?>) holder.getColumn());
      Assertions.assertNotNull(v4);
      List<NestedPathPart> path = ImmutableList.of(new NestedPathField("lastName"));
      ColumnHolder nestedColumnHolder = v4.getColumnHolder(path);
      Assertions.assertNotNull(nestedColumnHolder);
      Assertions.assertEquals(ColumnType.STRING, nestedColumnHolder.getCapabilities().toColumnType());
      NestedFieldDictionaryEncodedColumn<?> nestedColumn =
          (NestedFieldDictionaryEncodedColumn<?>) nestedColumnHolder.getColumn();
      Assertions.assertNotNull(nestedColumn);
      ColumnValueSelector<?> selector = nestedColumn.makeColumnValueSelector(
          new SimpleAscendingOffset(theIndex.getNumRows())
      );
      ColumnIndexSupplier indexSupplier = v4.getColumnIndexSupplier(path);
      Assertions.assertNotNull(indexSupplier);
      StringValueSetIndexes valueSetIndex = indexSupplier.as(StringValueSetIndexes.class);
      Assertions.assertNotNull(valueSetIndex);
      BitmapColumnIndex indexForValue = valueSetIndex.forValue(firstValue);
      Assertions.assertEquals(firstValue, selector.getObject());
      Assertions.assertTrue(indexForValue.computeBitmapResult(resultFactory, false).get(0));
    }
  }
}
