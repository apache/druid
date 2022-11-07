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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseProgressIndicator;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.NestedDataColumnIndexer;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NullValueIndex;
import org.apache.druid.segment.column.StringValueSetIndex;
import org.apache.druid.segment.column.TypeStrategy;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class NestedDataColumnSupplierTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  DefaultBitmapResultFactory resultFactory = new DefaultBitmapResultFactory(new RoaringBitmapFactory());

  List<Map<String, Object>> data = ImmutableList.of(
      ImmutableMap.of("x", 1L, "y", 1.0, "z", "a"),
      ImmutableMap.of("y", 3.0, "z", "d"),
      ImmutableMap.of("x", 5L, "y", 5.0, "z", "b"),
      ImmutableMap.of("x", 3L, "y", 4.0, "z", "c"),
      ImmutableMap.of("x", 2L),
      ImmutableMap.of("x", 4L, "y", 2.0, "z", "e")
  );

  Closer closer = Closer.create();

  SmooshedFileMapper fileMapper;

  ByteBuffer baseBuffer;

  @Before
  public void setup() throws IOException
  {
    final String fileNameBase = "test";
    TmpFileSegmentWriteOutMediumFactory writeOutMediumFactory = TmpFileSegmentWriteOutMediumFactory.instance();
    final File tmpFile = tempFolder.newFolder();
    try (final FileSmoosher smoosher = new FileSmoosher(tmpFile)) {


      NestedDataColumnSerializer serializer = new NestedDataColumnSerializer(
          fileNameBase,
          new IndexSpec(),
          writeOutMediumFactory.makeSegmentWriteOutMedium(tempFolder.newFolder()),
          new BaseProgressIndicator(),
          closer
      );

      NestedDataColumnIndexer indexer = new NestedDataColumnIndexer();
      for (Object o : data) {
        indexer.processRowValsToUnsortedEncodedKeyComponent(o, false);
      }
      SortedMap<String, NestedLiteralTypeInfo.MutableTypeSet> sortedFields = new TreeMap<>();
      indexer.mergeFields(sortedFields);

      GlobalDictionarySortedCollector globalDictionarySortedCollector = indexer.getSortedCollector();

      serializer.open();
      serializer.serializeFields(sortedFields);
      serializer.serializeStringDictionary(globalDictionarySortedCollector.getSortedStrings());
      serializer.serializeLongDictionary(globalDictionarySortedCollector.getSortedLongs());
      serializer.serializeDoubleDictionary(globalDictionarySortedCollector.getSortedDoubles());

      SettableSelector valueSelector = new SettableSelector();
      for (Object o : data) {
        valueSelector.setObject(StructuredData.wrap(o));
        serializer.serialize(valueSelector);
      }

      try (SmooshedWriter writer = smoosher.addWithSmooshedWriter(fileNameBase, serializer.getSerializedSize())) {
        serializer.writeTo(writer, smoosher);
      }
      smoosher.close();
      fileMapper = closer.register(SmooshedFileMapper.load(tmpFile));
      baseBuffer = fileMapper.mapFile(fileNameBase);
    }
  }

  @After
  public void teardown() throws IOException
  {
    closer.close();
  }

  @Test
  public void testBasicFunctionality() throws IOException
  {
    ColumnBuilder bob = new ColumnBuilder();
    bob.setFileMapper(fileMapper);
    NestedDataColumnSupplier supplier = new NestedDataColumnSupplier(
        baseBuffer,
        bob,
        () -> 0,
        NestedDataComplexTypeSerde.OBJECT_MAPPER,
        new OnlyPositionalReadsTypeStrategy<>(ColumnType.LONG.getStrategy()),
        new OnlyPositionalReadsTypeStrategy<>(ColumnType.DOUBLE.getStrategy())
    );
    try (NestedDataComplexColumn column = (NestedDataComplexColumn) supplier.get()) {
      smokeTest(column);
    }
  }

  @Test
  public void testConcurrency() throws ExecutionException, InterruptedException
  {
    // if this test ever starts being to be a flake, there might be thread safety issues
    ColumnBuilder bob = new ColumnBuilder();
    bob.setFileMapper(fileMapper);
    NestedDataColumnSupplier supplier = new NestedDataColumnSupplier(
        baseBuffer,
        bob,
        () -> 0,
        NestedDataComplexTypeSerde.OBJECT_MAPPER
    );
    final String expectedReason = "none";
    final AtomicReference<String> failureReason = new AtomicReference<>(expectedReason);

    final int threads = 10;
    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threads));
    Collection<ListenableFuture<?>> futures = new ArrayList<>(threads);
    final CountDownLatch threadsStartLatch = new CountDownLatch(1);
    for (int i = 0; i < threads; ++i) {
      futures.add(
          executorService.submit(() -> {
            try {
              threadsStartLatch.await();
              for (int iter = 0; iter < 5000; iter++) {
                try (NestedDataComplexColumn column = (NestedDataComplexColumn) supplier.get()) {
                  smokeTest(column);
                }
              }
            }
            catch (Throwable ex) {
              failureReason.set(ex.getMessage());
            }
          })
      );
    }
    threadsStartLatch.countDown();
    Futures.allAsList(futures).get();
    Assert.assertEquals(expectedReason, failureReason.get());
  }

  private void smokeTest(NestedDataComplexColumn column) throws IOException
  {
    SimpleAscendingOffset offset = new SimpleAscendingOffset(data.size());
    ColumnValueSelector<?> rawSelector = column.makeColumnValueSelector(offset);

    final List<NestedPathPart> xPath = NestedPathFinder.parseJsonPath("$.x");
    ColumnValueSelector<?> xSelector = column.makeColumnValueSelector(xPath, offset);
    ColumnIndexSupplier xIndexSupplier = column.getColumnIndexSupplier(xPath);
    Assert.assertNotNull(xIndexSupplier);
    StringValueSetIndex xValueIndex = xIndexSupplier.as(StringValueSetIndex.class);
    NullValueIndex xNulls = xIndexSupplier.as(NullValueIndex.class);

    final List<NestedPathPart> yPath = NestedPathFinder.parseJsonPath("$.y");
    ColumnValueSelector<?> ySelector = column.makeColumnValueSelector(yPath, offset);
    ColumnIndexSupplier yIndexSupplier = column.getColumnIndexSupplier(yPath);
    Assert.assertNotNull(yIndexSupplier);
    StringValueSetIndex yValueIndex = yIndexSupplier.as(StringValueSetIndex.class);
    NullValueIndex yNulls = yIndexSupplier.as(NullValueIndex.class);

    final List<NestedPathPart> zPath = NestedPathFinder.parseJsonPath("$.z");
    ColumnValueSelector<?> zSelector = column.makeColumnValueSelector(zPath, offset);
    ColumnIndexSupplier zIndexSupplier = column.getColumnIndexSupplier(zPath);
    Assert.assertNotNull(zIndexSupplier);
    StringValueSetIndex zValueIndex = zIndexSupplier.as(StringValueSetIndex.class);
    NullValueIndex zNulls = zIndexSupplier.as(NullValueIndex.class);

    for (int i = 0; i < data.size(); i++) {
      Map row = data.get(i);
      Assert.assertEquals(
          JSON_MAPPER.writeValueAsString(row),
          JSON_MAPPER.writeValueAsString(StructuredData.unwrap(rawSelector.getObject()))
      );
      if (row.containsKey("x")) {
        Assert.assertEquals(row.get("x"), xSelector.getObject());
        Assert.assertEquals(row.get("x"), xSelector.getLong());
        Assert.assertTrue(xValueIndex.forValue(String.valueOf(row.get("x"))).computeBitmapResult(resultFactory).get(i));
        Assert.assertFalse(xNulls.forNull().computeBitmapResult(resultFactory).get(i));
      } else {
        Assert.assertNull(xSelector.getObject());
        Assert.assertTrue(xSelector.isNull());
        Assert.assertTrue(xValueIndex.forValue(null).computeBitmapResult(resultFactory).get(i));
        Assert.assertTrue(xNulls.forNull().computeBitmapResult(resultFactory).get(i));
      }
      if (row.containsKey("y")) {
        Assert.assertEquals(row.get("y"), ySelector.getObject());
        Assert.assertEquals(row.get("y"), ySelector.getDouble());
        Assert.assertTrue(yValueIndex.forValue(String.valueOf(row.get("y"))).computeBitmapResult(resultFactory).get(i));
        Assert.assertFalse(yNulls.forNull().computeBitmapResult(resultFactory).get(i));
      } else {
        Assert.assertNull(ySelector.getObject());
        Assert.assertTrue(ySelector.isNull());
        Assert.assertTrue(yValueIndex.forValue(null).computeBitmapResult(resultFactory).get(i));
        Assert.assertTrue(yNulls.forNull().computeBitmapResult(resultFactory).get(i));
      }
      if (row.containsKey("z")) {
        Assert.assertEquals(row.get("z"), zSelector.getObject());
        Assert.assertTrue(zValueIndex.forValue((String) row.get("z")).computeBitmapResult(resultFactory).get(i));
        Assert.assertFalse(zNulls.forNull().computeBitmapResult(resultFactory).get(i));
      } else {
        Assert.assertNull(zSelector.getObject());
        Assert.assertTrue(zValueIndex.forValue(null).computeBitmapResult(resultFactory).get(i));
        Assert.assertTrue(zNulls.forNull().computeBitmapResult(resultFactory).get(i));
      }
      offset.increment();
    }
  }

  private static class SettableSelector extends ObjectColumnSelector<StructuredData>
  {
    private StructuredData data;

    public void setObject(StructuredData o)
    {
      this.data = o;
    }

    @Nullable
    @Override
    public StructuredData getObject()
    {
      return data;
    }

    @Override
    public Class classOfObject()
    {
      return StructuredData.class;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {

    }
  }

  private static class OnlyPositionalReadsTypeStrategy<T> implements TypeStrategy<T>
  {
    private final TypeStrategy<T> delegate;

    private OnlyPositionalReadsTypeStrategy(TypeStrategy<T> delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public int estimateSizeBytes(T value)
    {
      return delegate.estimateSizeBytes(value);
    }

    @Override
    public T read(ByteBuffer buffer)
    {
      throw new IllegalStateException("non-positional read");
    }

    @Override
    public boolean readRetainsBufferReference()
    {
      return delegate.readRetainsBufferReference();
    }

    @Override
    public int write(ByteBuffer buffer, T value, int maxSizeBytes)
    {
      return delegate.write(buffer, value, maxSizeBytes);
    }

    @Override
    public T read(ByteBuffer buffer, int offset)
    {
      return delegate.read(buffer, offset);
    }

    @Override
    public int write(ByteBuffer buffer, int offset, T value, int maxSizeBytes)
    {
      return delegate.write(buffer, offset, value, maxSizeBytes);
    }

    @Override
    public int compare(T o1, T o2)
    {
      return delegate.compare(o1, o2);
    }
  }
}
