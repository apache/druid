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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.filter.SelectorPredicateFactory;
import org.apache.druid.segment.AutoTypeColumnIndexer;
import org.apache.druid.segment.AutoTypeColumnMerger;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.IndexableAdapter;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.index.semantic.DruidPredicateIndexes;
import org.apache.druid.segment.index.semantic.NullValueIndex;
import org.apache.druid.segment.index.semantic.StringValueSetIndexes;
import org.apache.druid.segment.index.semantic.ValueIndexes;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class ScalarLongColumnSupplierTest extends InitializedNullHandlingTest
{
  private static final String NO_MATCH = "no";

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  BitmapSerdeFactory bitmapSerdeFactory = RoaringBitmapSerdeFactory.getInstance();
  DefaultBitmapResultFactory resultFactory = new DefaultBitmapResultFactory(bitmapSerdeFactory.getBitmapFactory());

  List<Long> data = Arrays.asList(
      1L,
      0L,
      null,
      2L,
      3L,
      4L
  );

  Closer closer = Closer.create();

  SmooshedFileMapper fileMapper;

  ByteBuffer baseBuffer;

  @BeforeClass
  public static void staticSetup()
  {
    NestedDataModule.registerHandlersAndSerde();
  }

  @Before
  public void setup() throws IOException
  {
    final String fileNameBase = "test";
    fileMapper = smooshify(fileNameBase, tempFolder.newFolder(), data);
    baseBuffer = fileMapper.mapFile(fileNameBase);
  }

  private SmooshedFileMapper smooshify(
      String fileNameBase,
      File tmpFile,
      List<?> data
  )
      throws IOException
  {
    SegmentWriteOutMediumFactory writeOutMediumFactory = TmpFileSegmentWriteOutMediumFactory.instance();
    try (final FileSmoosher smoosher = new FileSmoosher(tmpFile)) {
      ScalarLongColumnSerializer serializer = new ScalarLongColumnSerializer(
          fileNameBase,
          IndexSpec.DEFAULT,
          writeOutMediumFactory.makeSegmentWriteOutMedium(tempFolder.newFolder()),
          closer
      );

      AutoTypeColumnIndexer indexer = new AutoTypeColumnIndexer("test", null);
      for (Object o : data) {
        indexer.processRowValsToUnsortedEncodedKeyComponent(o, false);
      }
      SortedMap<String, FieldTypeInfo.MutableTypeSet> sortedFields = new TreeMap<>();

      IndexableAdapter.NestedColumnMergable mergable = closer.register(
          new IndexableAdapter.NestedColumnMergable(
              indexer.getSortedValueLookups(),
              indexer.getFieldTypeInfo(),
              false,
              false,
              null
          )
      );
      SortedValueDictionary globalDictionarySortedCollector = mergable.getValueDictionary();
      mergable.mergeFieldsInto(sortedFields);

      serializer.openDictionaryWriter();
      serializer.serializeDictionaries(
          globalDictionarySortedCollector.getSortedStrings(),
          globalDictionarySortedCollector.getSortedLongs(),
          globalDictionarySortedCollector.getSortedDoubles(),
          () -> new AutoTypeColumnMerger.ArrayDictionaryMergingIterator(
              new Iterable[]{globalDictionarySortedCollector.getSortedArrays()},
              serializer.getGlobalLookup()
          )
      );
      serializer.open();

      NestedDataColumnSupplierTest.SettableSelector valueSelector = new NestedDataColumnSupplierTest.SettableSelector();
      for (Object o : data) {
        valueSelector.setObject(StructuredData.wrap(o));
        serializer.serialize(valueSelector);
      }

      try (SmooshedWriter writer = smoosher.addWithSmooshedWriter(fileNameBase, serializer.getSerializedSize())) {
        serializer.writeTo(writer, smoosher);
      }
      smoosher.close();
      return closer.register(SmooshedFileMapper.load(tmpFile));
    }
  }

  @After
  public void teardown() throws IOException
  {
    closer.close();
  }

  @Test
  public void testBasicFunctionality()
  {
    ColumnBuilder bob = new ColumnBuilder();
    bob.setFileMapper(fileMapper);
    ScalarLongColumnAndIndexSupplier supplier = ScalarLongColumnAndIndexSupplier.read(
        ByteOrder.nativeOrder(),
        bitmapSerdeFactory,
        baseBuffer,
        bob,
        NestedFieldColumnIndexSupplierTest.ALWAYS_USE_INDEXES
    );
    try (ScalarLongColumn column = (ScalarLongColumn) supplier.get()) {
      smokeTest(supplier, column);
    }
  }

  @Test
  public void testConcurrency() throws ExecutionException, InterruptedException
  {
    // if this test ever starts being to be a flake, there might be thread safety issues
    ColumnBuilder bob = new ColumnBuilder();
    bob.setFileMapper(fileMapper);
    ScalarLongColumnAndIndexSupplier supplier = ScalarLongColumnAndIndexSupplier.read(
        ByteOrder.nativeOrder(),
        bitmapSerdeFactory,
        baseBuffer,
        bob,
        NestedFieldColumnIndexSupplierTest.ALWAYS_USE_INDEXES
    );
    final String expectedReason = "none";
    final AtomicReference<String> failureReason = new AtomicReference<>(expectedReason);

    final int threads = 10;
    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(threads, "StandardNestedColumnSupplierTest-%d")
    );
    Collection<ListenableFuture<?>> futures = new ArrayList<>(threads);
    final CountDownLatch threadsStartLatch = new CountDownLatch(1);
    for (int i = 0; i < threads; ++i) {
      futures.add(
          executorService.submit(() -> {
            try {
              threadsStartLatch.await();
              for (int iter = 0; iter < 5000; iter++) {
                try (ScalarLongColumn column = (ScalarLongColumn) supplier.get()) {
                  smokeTest(supplier, column);
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

  private void smokeTest(ScalarLongColumnAndIndexSupplier supplier, ScalarLongColumn column)
  {
    SimpleAscendingOffset offset = new SimpleAscendingOffset(data.size());
    NoFilterVectorOffset vectorOffset = new NoFilterVectorOffset(1, 0, data.size());
    ColumnValueSelector<?> valueSelector = column.makeColumnValueSelector(offset);
    VectorValueSelector vectorValueSelector = column.makeVectorValueSelector(vectorOffset);

    ValueIndexes valueIndexes = supplier.as(ValueIndexes.class);
    StringValueSetIndexes valueSetIndex = supplier.as(StringValueSetIndexes.class);
    DruidPredicateIndexes predicateIndex = supplier.as(DruidPredicateIndexes.class);
    NullValueIndex nullValueIndex = supplier.as(NullValueIndex.class);

    SortedMap<String, FieldTypeInfo.MutableTypeSet> fields = column.getFieldTypeInfo();
    Assert.assertEquals(
        ImmutableMap.of(NestedPathFinder.JSON_PATH_ROOT, new FieldTypeInfo.MutableTypeSet().add(ColumnType.LONG)),
        fields
    );

    for (int i = 0; i < data.size(); i++) {
      Long row = data.get(i);

      // in default value mode, even though the input row had an empty string, the selector spits out null, so we want
      // to take the null checking path

      if (row != null) {
        Assert.assertEquals(row, valueSelector.getObject());
        Assert.assertEquals((long) row, valueSelector.getLong());
        Assert.assertFalse(valueSelector.isNull());
        Assert.assertEquals((long) row, vectorValueSelector.getLongVector()[0]);
        Assert.assertEquals(row.doubleValue(), vectorValueSelector.getDoubleVector()[0], 0.0);
        Assert.assertEquals(row.floatValue(), vectorValueSelector.getFloatVector()[0], 0.0);
        boolean[] nullVector = vectorValueSelector.getNullVector();
        if (NullHandling.sqlCompatible() && nullVector != null) {
          Assert.assertFalse(nullVector[0]);
        } else {
          Assert.assertNull(nullVector);
        }

        Assert.assertTrue(valueSetIndex.forValue(String.valueOf(row)).computeBitmapResult(resultFactory, false).get(i));
        Assert.assertTrue(valueIndexes.forValue(row, ColumnType.LONG).computeBitmapResult(resultFactory, false).get(i));
        Assert.assertTrue(valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of(String.valueOf(row))))
                                       .computeBitmapResult(resultFactory, false)
                                       .get(i));
        Assert.assertTrue(predicateIndex.forPredicate(new SelectorPredicateFactory(String.valueOf(row)))
                                        .computeBitmapResult(resultFactory, false)
                                        .get(i));
        Assert.assertFalse(valueSetIndex.forValue(NO_MATCH).computeBitmapResult(resultFactory, false).get(i));
        Assert.assertFalse(valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of(NO_MATCH)))
                                        .computeBitmapResult(resultFactory, false)
                                        .get(i));
        Assert.assertFalse(predicateIndex.forPredicate(new SelectorPredicateFactory(NO_MATCH))
                                         .computeBitmapResult(resultFactory, false)
                                         .get(i));
        Assert.assertFalse(nullValueIndex.get().computeBitmapResult(resultFactory, false).get(i));

      } else {
        if (NullHandling.sqlCompatible()) {
          Assert.assertNull(valueSelector.getObject());
          Assert.assertTrue(valueSelector.isNull());
          Assert.assertTrue(vectorValueSelector.getNullVector()[0]);
          Assert.assertTrue(valueSetIndex.forValue(null).computeBitmapResult(resultFactory, false).get(i));
          Assert.assertTrue(nullValueIndex.get().computeBitmapResult(resultFactory, false).get(i));
          Assert.assertTrue(predicateIndex.forPredicate(new SelectorPredicateFactory(null))
                                          .computeBitmapResult(resultFactory, false)
                                          .get(i));
        } else {
          Assert.assertEquals(NullHandling.defaultLongValue(), valueSelector.getObject());
          Assert.assertFalse(valueSelector.isNull());
          Assert.assertNull(vectorValueSelector.getNullVector());
          Assert.assertFalse(valueSetIndex.forValue(null).computeBitmapResult(resultFactory, false).get(i));
          Assert.assertFalse(nullValueIndex.get().computeBitmapResult(resultFactory, false).get(i));
          Assert.assertFalse(predicateIndex.forPredicate(new SelectorPredicateFactory(null))
                                           .computeBitmapResult(resultFactory, false)
                                           .get(i));
          final String defaultString = String.valueOf(NullHandling.defaultLongValue());
          Assert.assertTrue(valueSetIndex.forValue(defaultString).computeBitmapResult(resultFactory, false).get(i));
          Assert.assertTrue(predicateIndex.forPredicate(new SelectorPredicateFactory(defaultString))
                                          .computeBitmapResult(resultFactory, false)
                                          .get(i));
        }

        Assert.assertFalse(valueSetIndex.forValue(NO_MATCH).computeBitmapResult(resultFactory, false).get(i));
        Assert.assertFalse(valueSetIndex.forValue(NO_MATCH).computeBitmapResult(resultFactory, false).get(i));
        Assert.assertFalse(predicateIndex.forPredicate(new SelectorPredicateFactory(NO_MATCH))
                                         .computeBitmapResult(resultFactory, false)
                                         .get(i));
      }

      offset.increment();
      vectorOffset.advance();
    }
  }
}
