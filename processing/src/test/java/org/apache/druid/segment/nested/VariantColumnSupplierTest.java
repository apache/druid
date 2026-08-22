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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
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
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.file.SegmentFileChannel;
import org.apache.druid.segment.index.semantic.ArrayElementIndexes;
import org.apache.druid.segment.index.semantic.DruidPredicateIndexes;
import org.apache.druid.segment.index.semantic.NullValueIndex;
import org.apache.druid.segment.index.semantic.StringValueSetIndexes;
import org.apache.druid.segment.index.semantic.ValueIndexes;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

@ParameterizedClass

@MethodSource("constructorFeeder")
public class VariantColumnSupplierTest extends InitializedNullHandlingTest
{
  @RegisterExtension
  public final TemporaryFolderExtension tempFolder = new TemporaryFolderExtension();

  BitmapSerdeFactory bitmapSerdeFactory = RoaringBitmapSerdeFactory.getInstance();
  DefaultBitmapResultFactory resultFactory = new DefaultBitmapResultFactory(bitmapSerdeFactory.getBitmapFactory());
  static List<List<Long>> LONG_ARRAY = Arrays.asList(
      Collections.emptyList(),
      Arrays.asList(1L, null, 2L),
      null,
      Collections.singletonList(null),
      Arrays.asList(3L, 4L),
      Arrays.asList(null, null)
  );

  static List<List<Double>> DOUBLE_ARRAY = Arrays.asList(
      Collections.emptyList(),
      Arrays.asList(1.1, null, 2.2),
      null,
      Collections.singletonList(null),
      Arrays.asList(3.3, 4.4),
      Arrays.asList(null, null)
  );

  static List<List<String>> STRING_ARRAY = Arrays.asList(
      Collections.emptyList(),
      Arrays.asList("a", null, "b"),
      null,
      Collections.singletonList(null),
      Arrays.asList("c", "d"),
      Arrays.asList(null, null)
  );

  static List<Object> VARIANT_NUMERIC = Arrays.asList(
      1L,
      2.2,
      null,
      3.3,
      4L,
      null
  );

  static List<Object> VARIANT_SCALAR = Arrays.asList(
      null,
      1L,
      null,
      "b",
      3.3,
      4L
  );

  static List<Object> VARIANT_SCALAR_AND_ARRAY = Arrays.asList(
      Collections.emptyList(),
      2L,
      null,
      Collections.singletonList(null),
      Arrays.asList(3L, 4L),
      Arrays.asList(null, "a"),
      5.5,
      "b"
  );

  static List<List<Object>> VARIANT_ARRAY = Arrays.asList(
      Collections.emptyList(),
      Arrays.asList("a", null, "b"),
      null,
      Collections.singletonList(null),
      Arrays.asList(3L, 4L),
      Arrays.asList(null, 3.3)
  );

  static List<List<Object>> NO_TYPE_ARRAY = Arrays.asList(
      Collections.emptyList(),
      null,
      Collections.emptyList(),
      Arrays.asList(null, null)
  );


  @BeforeAll
  public static void staticSetup()
  {
    BuiltInTypesModule.registerHandlersAndSerde();
  }
  public static Collection<?> constructorFeeder()
  {

    NestedCommonFormatColumnFormatSpec defaultSpec = NestedCommonFormatColumnFormatSpec.builder().build();

    NestedCommonFormatColumnFormatSpec frontCodedDict =
        NestedCommonFormatColumnFormatSpec.builder()
                                          .setStringDictionaryEncoding(
                                              new StringEncodingStrategy.FrontCoded(16, FrontCodedIndexed.V1)
                                          )
                                          .build();
    final List<Object[]> constructors = ImmutableList.of(
        new Object[]{"ARRAY<LONG>", LONG_ARRAY, defaultSpec},
        new Object[]{"ARRAY<LONG>", LONG_ARRAY, frontCodedDict},
        new Object[]{"ARRAY<DOUBLE>", DOUBLE_ARRAY, defaultSpec},
        new Object[]{"ARRAY<DOUBLE>", DOUBLE_ARRAY, frontCodedDict},
        new Object[]{"ARRAY<STRING>", STRING_ARRAY, defaultSpec},
        new Object[]{"ARRAY<STRING>", STRING_ARRAY, frontCodedDict},
        new Object[]{"DOUBLE,LONG", VARIANT_NUMERIC, defaultSpec},
        new Object[]{"DOUBLE,LONG", VARIANT_NUMERIC, frontCodedDict},
        new Object[]{"DOUBLE,LONG,STRING", VARIANT_SCALAR, defaultSpec},
        new Object[]{"DOUBLE,LONG,STRING", VARIANT_SCALAR, frontCodedDict},
        new Object[]{"ARRAY<LONG>,ARRAY<STRING>,DOUBLE,LONG,STRING", VARIANT_SCALAR_AND_ARRAY, defaultSpec},
        new Object[]{"ARRAY<LONG>,ARRAY<STRING>,DOUBLE,LONG,STRING", VARIANT_SCALAR_AND_ARRAY, frontCodedDict},
        new Object[]{"ARRAY<DOUBLE>,ARRAY<LONG>,ARRAY<STRING>", VARIANT_ARRAY, defaultSpec},
        new Object[]{"ARRAY<DOUBLE>,ARRAY<LONG>,ARRAY<STRING>", VARIANT_ARRAY, frontCodedDict},
        new Object[]{"ARRAY<LONG>", NO_TYPE_ARRAY, defaultSpec},
        new Object[]{"ARRAY<LONG>", NO_TYPE_ARRAY, frontCodedDict}
    );

    return constructors;
  }

  Closer closer = Closer.create();

  SmooshedFileMapper fileMapper;

  ByteBuffer baseBuffer;

  FieldTypeInfo.MutableTypeSet expectedTypes;

  ColumnType expectedLogicalType = null;

  private final List<?> data;
  private final NestedCommonFormatColumnFormatSpec columnFormatSpec;

  public VariantColumnSupplierTest(
      @SuppressWarnings("unused") String name,
      List<?> data,
      NestedCommonFormatColumnFormatSpec columnFormatSpec
  )
  {
    this.data = data;
    this.columnFormatSpec = columnFormatSpec;
  }

  @BeforeEach
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

      AutoTypeColumnIndexer indexer = new AutoTypeColumnIndexer("test", null, columnFormatSpec);
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

      expectedTypes = new FieldTypeInfo.MutableTypeSet((byte) (sortedFields.get(NestedPathFinder.JSON_PATH_ROOT).getByteValue() & 0x7F));
      for (ColumnType type : FieldTypeInfo.convertToSet(expectedTypes.getByteValue())) {
        expectedLogicalType = ColumnType.leastRestrictiveType(expectedLogicalType, type);
      }
      if (expectedLogicalType == null && sortedFields.get(NestedPathFinder.JSON_PATH_ROOT).hasUntypedArray()) {
        expectedLogicalType = ColumnType.LONG_ARRAY;
      }
      VariantColumnSerializer serializer = new VariantColumnSerializer(
          fileNameBase,
          expectedTypes.getSingleType() == null ? null : expectedLogicalType,
          expectedTypes.getSingleType() == null ? expectedTypes.getByteValue() : null,
          NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(columnFormatSpec, IndexSpec.getDefault().getEffectiveSpec()),
          writeOutMediumFactory.makeSegmentWriteOutMedium(tempFolder.newFolder()),
          closer
      );

      serializer.openDictionaryWriter(tempFolder.newFolder());
      serializer.serializeDictionaries(
          globalDictionarySortedCollector.getSortedStrings(),
          globalDictionarySortedCollector.getSortedLongs(),
          globalDictionarySortedCollector.getSortedDoubles(),
          () -> new AutoTypeColumnMerger.ArrayDictionaryMergingIterator(
              new Iterable[]{globalDictionarySortedCollector.getSortedArrays()},
              serializer.getDictionaryIdLookup()
          )
      );
      serializer.open();

      NestedDataColumnSupplierTest.SettableSelector valueSelector = new NestedDataColumnSupplierTest.SettableSelector();
      for (Object o : data) {
        valueSelector.setObject(StructuredData.wrap(o));
        serializer.serialize(valueSelector);
      }

      try (SegmentFileChannel writer = smoosher.addWithChannel(fileNameBase, serializer.getSerializedSize())) {
        serializer.writeTo(writer, smoosher);
      }
      smoosher.close();
      return closer.register(SmooshedFileMapper.load(tmpFile));
    }
  }

  @AfterEach
  public void teardown() throws IOException
  {
    closer.close();
  }

  @Test
  public void testBasicFunctionality() throws IOException
  {
    ColumnBuilder bob = new ColumnBuilder();
    bob.setFileMapper(fileMapper);
    VariantColumnAndIndexSupplier supplier = VariantColumnAndIndexSupplier.read(
        expectedLogicalType,
        ByteOrder.nativeOrder(),
        bitmapSerdeFactory,
        baseBuffer,
        bob.getFileMapper(),
        null
    );
    try (VariantColumn<?> column = (VariantColumn<?>) supplier.get()) {
      smokeTest(supplier, column, data, expectedTypes);
    }
  }

  @Test
  public void testConcurrency() throws ExecutionException, InterruptedException
  {
    // if this test ever starts being to be a flake, there might be thread safety issues
    ColumnBuilder bob = new ColumnBuilder();
    bob.setFileMapper(fileMapper);
    VariantColumnAndIndexSupplier supplier = VariantColumnAndIndexSupplier.read(
        expectedLogicalType,
        ByteOrder.nativeOrder(),
        bitmapSerdeFactory,
        baseBuffer,
        bob.getFileMapper(),
        null
    );
    final String expectedReason = "none";
    final AtomicReference<String> failureReason = new AtomicReference<>(expectedReason);

    final int threads = 10;
    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(threads, "StandardNestedColumnSupplierTest-%d")
    );
    try {
      Collection<ListenableFuture<?>> futures = new ArrayList<>(threads);
      final CountDownLatch threadsStartLatch = new CountDownLatch(1);
      for (int i = 0; i < threads; ++i) {
        futures.add(
            executorService.submit(() -> {
              try {
                threadsStartLatch.await();
                for (int iter = 0; iter < 5000; iter++) {
                  try (VariantColumn column = (VariantColumn) supplier.get()) {
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
      Assertions.assertEquals(expectedReason, failureReason.get());
    }
    finally {
      executorService.shutdownNow();
    }
  }

  private void smokeTest(
      VariantColumnAndIndexSupplier supplier,
      VariantColumn<?> column,
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

    StringValueSetIndexes valueSetIndex = supplier.as(StringValueSetIndexes.class);
    Assertions.assertNull(valueSetIndex);
    DruidPredicateIndexes predicateIndex = supplier.as(DruidPredicateIndexes.class);
    Assertions.assertNull(predicateIndex);
    NullValueIndex nullValueIndex = supplier.as(NullValueIndex.class);
    Assertions.assertNotNull(nullValueIndex);
    ValueIndexes valueIndexes = supplier.as(ValueIndexes.class);
    ArrayElementIndexes arrayElementIndexes = supplier.as(ArrayElementIndexes.class);
    if (expectedType.getSingleType() != null && expectedType.getSingleType().isArray()) {
      Assertions.assertNotNull(valueIndexes);
      Assertions.assertNotNull(arrayElementIndexes);
    } else {
      Assertions.assertNull(valueIndexes);
      Assertions.assertNull(arrayElementIndexes);
    }

    SortedMap<String, FieldTypeInfo.MutableTypeSet> fields = column.getFieldTypeInfo();
    Assertions.assertEquals(1, fields.size());
    Assertions.assertEquals(
        expectedType,
        fields.get(NestedPathFinder.JSON_PATH_ROOT)
    );
    final ExpressionType expressionType = ExpressionType.fromColumnTypeStrict(expectedLogicalType);

    for (int i = 0; i < data.size(); i++) {
      Object row = data.get(i);

      // in default value mode, even though the input row had an empty string, the selector spits out null, so we want
      // to take the null checking path

      if (row != null) {
        if (row instanceof List) {
          Assertions.assertArrayEquals(((List) row).toArray(), (Object[]) valueSelector.getObject());
          if (expectedType.getSingleType() != null) {
            Assertions.assertArrayEquals(((List) row).toArray(), (Object[]) vectorObjectSelector.getObjectVector()[0]);
            Assertions.assertTrue(valueIndexes.forValue(row, expectedType.getSingleType()).computeBitmapResult(resultFactory,
                                                                                                           false
            ).get(i));
            for (Object o : ((List) row)) {
              Assertions.assertTrue(
                  arrayElementIndexes.containsValue(o, expectedType.getSingleType().getElementType())
                                      .computeBitmapResult(resultFactory, false)
                                      .get(i),
                  "Failed on row: " + row
              );
            }
          } else {
            // mixed type vector object selector coerces to the most common type
            Assertions.assertArrayEquals(ExprEval.ofType(expressionType, row).asArray(), (Object[]) vectorObjectSelector.getObjectVector()[0]);
          }
        } else {
          Assertions.assertEquals(row, valueSelector.getObject());
          if (expectedType.getSingleType() != null) {
            Assertions.assertEquals(
                row,
                vectorObjectSelector.getObjectVector()[0]
            );
          } else {
            // vector object selector always coerces to the most common type
            ExprEval eval = ExprEval.ofType(expressionType, row);
            if (expectedLogicalType.isArray()) {
              Assertions.assertArrayEquals(eval.asArray(), (Object[]) vectorObjectSelector.getObjectVector()[0]);
            } else {
              Assertions.assertEquals(eval.value(), vectorObjectSelector.getObjectVector()[0]);
            }
          }
          if (dimensionSelector != null) {
            Assertions.assertEquals(String.valueOf(row), dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
            // null is always 0
            Assertions.assertTrue(dimensionSelector.idLookup().lookupId(String.valueOf(row)) > 0);
            if (dimensionVectorSelector != null) {
              int[] dim = dimensionVectorSelector.getRowVector();
              Assertions.assertEquals(String.valueOf(row), dimensionVectorSelector.lookupName(dim[0]));
            }
          }
        }
        Assertions.assertFalse(nullValueIndex.get().computeBitmapResult(resultFactory, false).get(i));

      } else {
        Assertions.assertNull(valueSelector.getObject());
        Assertions.assertNull(vectorObjectSelector.getObjectVector()[0]);
        if (dimensionSelector != null) {
          Assertions.assertNull(dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
          Assertions.assertEquals(0, dimensionSelector.idLookup().lookupId(null));
          if (dimensionVectorSelector != null) {
            Assertions.assertNull(dimensionVectorSelector.lookupName(dimensionVectorSelector.getRowVector()[0]));
          }
        }
        Assertions.assertTrue(nullValueIndex.get().computeBitmapResult(resultFactory, false).get(i));
        if (expectedType.getSingleType() != null) {
          Assertions.assertFalse(arrayElementIndexes.containsValue(null, expectedType.getSingleType()).computeBitmapResult(resultFactory, false).get(i));
        }
      }

      offset.increment();
      vectorOffset.advance();
    }
  }
}
