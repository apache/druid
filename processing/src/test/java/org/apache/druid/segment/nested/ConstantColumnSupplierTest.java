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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.segment.AutoTypeColumnIndexer;
import org.apache.druid.segment.AutoTypeColumnMerger;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.IndexableAdapter;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.DruidPredicateIndex;
import org.apache.druid.segment.column.LexicographicalRangeIndex;
import org.apache.druid.segment.column.NullValueIndex;
import org.apache.druid.segment.column.NumericRangeIndex;
import org.apache.druid.segment.column.StringValueSetIndex;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(Parameterized.class)
public class ConstantColumnSupplierTest extends InitializedNullHandlingTest
{

  public static List<Object> CONSTANT_NULL = Arrays.asList(
      null,
      null,
      null,
      null,
      null
  );

  public static List<String> CONSTANT_STRING = Arrays.asList(
      "abcd",
      "abcd",
      "abcd",
      "abcd",
      "abcd"
  );

  public static List<Long> CONSTANT_LONG = Arrays.asList(
      1234L,
      1234L,
      1234L,
      1234L,
      1234L
  );

  public static List<List<Long>> CONSTANT_LONG_ARRAY = Arrays.asList(
      Arrays.asList(1L, 2L, 3L, 4L),
      Arrays.asList(1L, 2L, 3L, 4L),
      Arrays.asList(1L, 2L, 3L, 4L),
      Arrays.asList(1L, 2L, 3L, 4L),
      Arrays.asList(1L, 2L, 3L, 4L)
  );

  public static List<Object> CONSTANT_EMPTY_OBJECTS = Arrays.asList(
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap()
  );

  public static List<Object> CONSTANT_OBJECTS = Arrays.asList(
      ImmutableMap.of("x", 1234, "y", 1.234, "z", "abcd"),
      ImmutableMap.of("x", 1234, "y", 1.234, "z", "abcd"),
      ImmutableMap.of("x", 1234, "y", 1.234, "z", "abcd"),
      ImmutableMap.of("x", 1234, "y", 1.234, "z", "abcd"),
      ImmutableMap.of("x", 1234, "y", 1.234, "z", "abcd")
  );

  @BeforeClass
  public static void staticSetup()
  {
    NestedDataModule.registerHandlersAndSerde();
  }

  @Parameterized.Parameters(name = "data = {0}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = ImmutableList.of(
        new Object[]{"NULL", CONSTANT_NULL, IndexSpec.DEFAULT},
        new Object[]{"STRING", CONSTANT_STRING, IndexSpec.DEFAULT},
        new Object[]{"LONG", CONSTANT_LONG, IndexSpec.DEFAULT},
        new Object[]{"ARRAY<LONG>", CONSTANT_LONG_ARRAY, IndexSpec.DEFAULT},
        new Object[]{"EMPTY OBJECT", CONSTANT_EMPTY_OBJECTS, IndexSpec.DEFAULT},
        new Object[]{"OBJECT", CONSTANT_OBJECTS, IndexSpec.DEFAULT}
    );

    return constructors;
  }

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  Closer closer = Closer.create();

  SmooshedFileMapper fileMapper;

  ByteBuffer baseBuffer;

  FieldTypeInfo.MutableTypeSet expectedTypes;

  ColumnType expectedLogicalType = null;

  private final List<?> data;
  private final IndexSpec indexSpec;

  BitmapSerdeFactory bitmapSerdeFactory = RoaringBitmapSerdeFactory.getInstance();
  DefaultBitmapResultFactory resultFactory = new DefaultBitmapResultFactory(bitmapSerdeFactory.getBitmapFactory());

  public ConstantColumnSupplierTest(
      @SuppressWarnings("unused") String name,
      List<?> data,
      IndexSpec indexSpec
  )
  {
    this.data = data;
    this.indexSpec = indexSpec;
  }

  @Before
  public void setup() throws IOException
  {
    final String fileNameBase = "test";
    fileMapper = smooshify(fileNameBase, tempFolder.newFolder());
    baseBuffer = fileMapper.mapFile(fileNameBase);
  }

  private SmooshedFileMapper smooshify(
      String fileNameBase,
      File tmpFile
  )
      throws IOException
  {
    SegmentWriteOutMediumFactory writeOutMediumFactory = TmpFileSegmentWriteOutMediumFactory.instance();
    try (final FileSmoosher smoosher = new FileSmoosher(tmpFile)) {

      AutoTypeColumnIndexer indexer = new AutoTypeColumnIndexer();
      for (Object o : data) {
        indexer.processRowValsToUnsortedEncodedKeyComponent(o, false);
      }
      SortedMap<String, FieldTypeInfo.MutableTypeSet> sortedFields = new TreeMap<>();

      IndexableAdapter.NestedColumnMergable mergable = closer.register(
          new IndexableAdapter.NestedColumnMergable(
              indexer.getSortedValueLookups(),
              indexer.getFieldTypeInfo(),
              data.get(0) instanceof Map,
              true,
              data.get(0)
          )
      );
      SortedValueDictionary globalDictionarySortedCollector = mergable.getValueDictionary();
      mergable.mergeFieldsInto(sortedFields);

      expectedTypes = sortedFields.get(NestedPathFinder.JSON_PATH_ROOT);
      if (expectedTypes != null) {
        for (ColumnType type : FieldTypeInfo.convertToSet(expectedTypes.getByteValue())) {
          expectedLogicalType = ColumnType.leastRestrictiveType(expectedLogicalType, type);
        }
        if (expectedLogicalType == null) {
          expectedLogicalType = ColumnType.STRING;
        }
      } else {
        expectedLogicalType = ColumnType.STRING;
      }
      if (data.get(0) instanceof Map) {
        expectedLogicalType = ColumnType.NESTED_DATA;
      }
      ConstantColumnSerializer serializer = new ConstantColumnSerializer(
          fileNameBase,
          data.get(0)
      );

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
  public void testBasicFunctionality() throws IOException
  {
    ColumnBuilder bob = new ColumnBuilder();
    bob.setFileMapper(fileMapper);
    ConstantColumnAndIndexSupplier supplier = ConstantColumnAndIndexSupplier.read(
        expectedLogicalType,
        bitmapSerdeFactory,
        baseBuffer
    );
    try (ConstantColumn column = (ConstantColumn) supplier.get()) {
      smokeTest(supplier, column, data, expectedTypes);
    }
  }

  @Test
  public void testConcurrency() throws ExecutionException, InterruptedException
  {
    // if this test ever starts being to be a flake, there might be thread safety issues
    ColumnBuilder bob = new ColumnBuilder();
    bob.setFileMapper(fileMapper);
    ConstantColumnAndIndexSupplier supplier = ConstantColumnAndIndexSupplier.read(
        expectedLogicalType,
        bitmapSerdeFactory,
        baseBuffer
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
                try (ConstantColumn column = (ConstantColumn) supplier.get()) {
                  smokeTest(supplier, column, data, expectedTypes);
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

  private void smokeTest(
      ConstantColumnAndIndexSupplier supplier,
      ConstantColumn column,
      List<?> data,
      FieldTypeInfo.MutableTypeSet expectedType
  )
  {
    SimpleAscendingOffset offset = new SimpleAscendingOffset(data.size());
    NoFilterVectorOffset vectorOffset = new NoFilterVectorOffset(1, 0, data.size());
    ColumnValueSelector<?> valueSelector = column.makeColumnValueSelector(offset);
    DimensionSelector dimensionSelector =
        expectedLogicalType.isPrimitive() ? column.makeDimensionSelector(offset, null) : null;
    VectorObjectSelector vectorObjectSelector = column.makeVectorObjectSelector(vectorOffset);
    SingleValueDimensionVectorSelector dimensionVectorSelector =
        expectedLogicalType.isPrimitive() ? column.makeSingleValueDimensionVectorSelector(vectorOffset) : null;

    StringValueSetIndex valueSetIndex = supplier.as(StringValueSetIndex.class);
    DruidPredicateIndex predicateIndex = supplier.as(DruidPredicateIndex.class);
    NullValueIndex nullValueIndex = supplier.as(NullValueIndex.class);
    LexicographicalRangeIndex lexicographicalRangeIndex = supplier.as(LexicographicalRangeIndex.class);
    NumericRangeIndex rangeIndex = supplier.as(NumericRangeIndex.class);
    if (!expectedLogicalType.isPrimitive()) {
      Assert.assertNull(valueSetIndex);
      Assert.assertNull(predicateIndex);
      Assert.assertNull(lexicographicalRangeIndex);
      Assert.assertNull(rangeIndex);
    } else {
      Assert.assertNotNull(valueSetIndex);
      Assert.assertNotNull(predicateIndex);
      if (expectedLogicalType.isNumeric()) {
        Assert.assertNull(lexicographicalRangeIndex);
        Assert.assertNotNull(rangeIndex);
      } else {
        Assert.assertNotNull(lexicographicalRangeIndex);
        Assert.assertNull(rangeIndex);
      }
    }
    Assert.assertNotNull(nullValueIndex);

    SortedMap<String, FieldTypeInfo.MutableTypeSet> fields = column.getFieldTypeInfo();
    if (expectedType != null) {
      Assert.assertEquals(
          ImmutableMap.of(NestedPathFinder.JSON_PATH_ROOT, expectedType),
          fields
      );
    }
    final ExpressionType expressionType = ExpressionType.fromColumnTypeStrict(expectedLogicalType);

    for (int i = 0; i < data.size(); i++) {
      Object row = data.get(i);

      // in default value mode, even though the input row had an empty string, the selector spits out null, so we want
      // to take the null checking path

      if (row != null) {
        if (row instanceof List) {
          Assert.assertArrayEquals(((List) row).toArray(), (Object[]) valueSelector.getObject());
          if (expectedType.getSingleType() != null) {
            Assert.assertArrayEquals(((List) row).toArray(), (Object[]) vectorObjectSelector.getObjectVector()[0]);
          } else {
            // mixed type vector object selector coerces to the most common type
            Assert.assertArrayEquals(ExprEval.ofType(expressionType, row).asArray(), (Object[]) vectorObjectSelector.getObjectVector()[0]);
          }
        } else {
          if (row instanceof Map) {
            assertMapEquals(row, valueSelector.getObject());
          } else {
            Assert.assertEquals(row, valueSelector.getObject());
            Assert.assertTrue(valueSetIndex.forValue(String.valueOf(row)).computeBitmapResult(resultFactory).get(i));
          }
          if (expectedType != null && expectedType.getSingleType() != null) {
            Assert.assertEquals(
                row,
                vectorObjectSelector.getObjectVector()[0]
            );
          } else {
            // vector object selector always coerces to the most common type
            ExprEval eval = ExprEval.ofType(expressionType, row);
            if (expectedLogicalType.isArray()) {
              Assert.assertArrayEquals(eval.asArray(), (Object[]) vectorObjectSelector.getObjectVector()[0]);
            } else if (expectedLogicalType.isPrimitive()) {
              assertMapEquals(eval.value(), vectorObjectSelector.getObjectVector()[0]);
            } else {
              Assert.assertEquals(eval.value(), vectorObjectSelector.getObjectVector()[0]);
            }
          }
          if (dimensionSelector != null) {
            Assert.assertEquals(String.valueOf(row), dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
            // constants, always 0
            Assert.assertTrue(dimensionSelector.idLookup().lookupId(String.valueOf(row)) == 0);
            if (dimensionVectorSelector != null) {
              int[] dim = dimensionVectorSelector.getRowVector();
              Assert.assertEquals(String.valueOf(row), dimensionVectorSelector.lookupName(dim[0]));
            }
          }
        }
        Assert.assertFalse(nullValueIndex.forNull().computeBitmapResult(resultFactory).get(i));

      } else {
        Assert.assertNull(valueSelector.getObject());
        Assert.assertNull(vectorObjectSelector.getObjectVector()[0]);
        if (dimensionSelector != null) {
          Assert.assertNull(dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
          Assert.assertEquals(0, dimensionSelector.idLookup().lookupId(null));
          if (dimensionVectorSelector != null) {
            Assert.assertNull(dimensionVectorSelector.lookupName(dimensionVectorSelector.getRowVector()[0]));
          }
        }
        Assert.assertTrue(nullValueIndex.forNull().computeBitmapResult(resultFactory).get(i));
      }

      offset.increment();
      vectorOffset.advance();
    }
  }

  private static void assertMapEquals(Object o1, Object o2)
  {
    Map<String, ?> m1 = (Map<String, ?>) o1;
    Map<String, ?> m2 = (Map<String, ?>) o2;
    Assert.assertEquals(m1.size(), m2.size());
    for (String k : m1.keySet()) {
      Assert.assertEquals(m1.get(k), m2.get(k));
    }
  }
}
