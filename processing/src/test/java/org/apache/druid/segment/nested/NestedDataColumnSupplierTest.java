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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.WrappedRoaringBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.filter.SelectorPredicateFactory;
import org.apache.druid.query.filter.StringPredicateDruidPredicateFactory;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AutoTypeColumnIndexer;
import org.apache.druid.segment.AutoTypeColumnMerger;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.IndexableAdapter;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.index.semantic.DruidPredicateIndexes;
import org.apache.druid.segment.index.semantic.NullValueIndex;
import org.apache.druid.segment.index.semantic.StringValueSetIndexes;
import org.apache.druid.segment.serde.ColumnPartSerde;
import org.apache.druid.segment.serde.NestedCommonFormatColumnPartSerde;
import org.apache.druid.segment.vector.BitmapVectorOffset;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.VectorObjectSelector;
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

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class NestedDataColumnSupplierTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  private static final String NO_MATCH = "no";

  private static final ColumnConfig ALWAYS_USE_INDEXES = new ColumnConfig()
  {

    @Override
    public double skipValueRangeIndexScale()
    {
      return 1.0;
    }

    @Override
    public double skipValuePredicateIndexScale()
    {
      return 1.0;
    }
  };

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  BitmapSerdeFactory bitmapSerdeFactory = RoaringBitmapSerdeFactory.getInstance();
  DefaultBitmapResultFactory resultFactory = new DefaultBitmapResultFactory(bitmapSerdeFactory.getBitmapFactory());

  List<Map<String, Object>> data = ImmutableList.of(
      TestHelper.makeMap("x", 1L, "y", 1.0, "z", "a", "v", "100", "nullish", "notnull"),
      TestHelper.makeMap("y", 3.0, "z", "d", "v", 1000L, "nullish", null),
      TestHelper.makeMap("x", 5L, "y", 5.0, "z", "b", "nullish", ""),
      TestHelper.makeMap("x", 3L, "y", 4.0, "z", "c", "v", 3000.333, "nullish", "null"),
      TestHelper.makeMap("x", 2L, "v", "40000"),
      TestHelper.makeMap("x", 4L, "y", 2.0, "z", "e", "v", 11111L, "nullish", null)
  );

  List<Map<String, Object>> arrayTestData = ImmutableList.of(
      TestHelper.makeMap("s", new Object[]{"a", "b", "c"}, "l", new Object[]{1L, 2L, 3L}, "d", new Object[]{1.1, 2.2}),
      TestHelper.makeMap(
          "s",
          new Object[]{null, "b", "c"},
          "l",
          new Object[]{1L, null, 3L},
          "d",
          new Object[]{2.2, 2.2}
      ),
      TestHelper.makeMap(
          "s",
          new Object[]{"b", "c"},
          "l",
          new Object[]{null, null},
          "d",
          new Object[]{1.1, null, 2.2}
      ),
      TestHelper.makeMap("s", new Object[]{"a", "b", "c", "d"}, "l", new Object[]{4L, 2L, 3L}),
      TestHelper.makeMap("s", new Object[]{"d", "b", "c", "a"}, "d", new Object[]{1.1, 2.2}),
      TestHelper.makeMap("l", new Object[]{1L, 2L, 3L}, "d", new Object[]{3.1, 2.2, 1.9})
  );

  Closer closer = Closer.create();

  SmooshedFileMapper fileMapper;

  ByteBuffer baseBuffer;

  SmooshedFileMapper arrayFileMapper;

  ByteBuffer arrayBaseBuffer;

  @BeforeClass
  public static void staticSetup()
  {
    NestedDataModule.registerHandlersAndSerde();
  }

  @Before
  public void setup() throws IOException
  {
    final String fileNameBase = "test/column";
    final String arrayFileNameBase = "array";
    fileMapper = smooshify(fileNameBase, tempFolder.newFolder(), data);
    baseBuffer = fileMapper.mapFile(fileNameBase);
    arrayFileMapper = smooshify(arrayFileNameBase, tempFolder.newFolder(), arrayTestData);
    arrayBaseBuffer = arrayFileMapper.mapFile(arrayFileNameBase);
  }

  private SmooshedFileMapper smooshify(
      String fileNameBase,
      File tmpFile,
      List<Map<String, Object>> data
  )
      throws IOException
  {
    SegmentWriteOutMediumFactory writeOutMediumFactory = TmpFileSegmentWriteOutMediumFactory.instance();
    try (final FileSmoosher smoosher = new FileSmoosher(tmpFile)) {
      NestedDataColumnSerializer serializer = new NestedDataColumnSerializer(
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
      serializer.serializeFields(sortedFields);
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

      SettableSelector valueSelector = new SettableSelector();
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
    NestedCommonFormatColumnPartSerde partSerde = NestedCommonFormatColumnPartSerde.createDeserializer(
        ColumnType.NESTED_DATA,
        false,
        false,
        false,
        ByteOrder.nativeOrder(),
        RoaringBitmapSerdeFactory.getInstance()
    );
    bob.setFileMapper(fileMapper);
    ColumnPartSerde.Deserializer deserializer = partSerde.getDeserializer();
    deserializer.read(baseBuffer, bob, ALWAYS_USE_INDEXES);
    final ColumnHolder holder = bob.build();
    final ColumnCapabilities capabilities = holder.getCapabilities();
    Assert.assertEquals(ColumnType.NESTED_DATA, capabilities.toColumnType());
    Assert.assertTrue(holder.getColumnFormat() instanceof NestedCommonFormatColumn.Format);
    try (NestedDataComplexColumn column = (NestedDataComplexColumn) holder.getColumn()) {
      smokeTest(column);
    }
  }

  @Test
  public void testArrayFunctionality() throws IOException
  {
    ColumnBuilder bob = new ColumnBuilder();
    NestedCommonFormatColumnPartSerde partSerde = NestedCommonFormatColumnPartSerde.createDeserializer(
        ColumnType.NESTED_DATA,
        false,
        false,
        false,
        ByteOrder.nativeOrder(),
        RoaringBitmapSerdeFactory.getInstance()
    );
    bob.setFileMapper(arrayFileMapper);
    ColumnPartSerde.Deserializer deserializer = partSerde.getDeserializer();
    deserializer.read(arrayBaseBuffer, bob, ALWAYS_USE_INDEXES);
    final ColumnHolder holder = bob.build();
    final ColumnCapabilities capabilities = holder.getCapabilities();
    Assert.assertEquals(ColumnType.NESTED_DATA, capabilities.toColumnType());
    Assert.assertTrue(holder.getColumnFormat() instanceof NestedCommonFormatColumn.Format);
    try (NestedDataComplexColumn column = (NestedDataComplexColumn) holder.getColumn()) {
      smokeTestArrays(column);
    }
  }

  @Test
  public void testConcurrency() throws ExecutionException, InterruptedException
  {
    // if this test ever starts being to be a flake, there might be thread safety issues
    ColumnBuilder bob = new ColumnBuilder();
    bob.setFileMapper(fileMapper);
    NestedDataColumnSupplier supplier = NestedDataColumnSupplier.read(
        ColumnType.NESTED_DATA,
        false,
        baseBuffer,
        bob,
        ALWAYS_USE_INDEXES,
        bitmapSerdeFactory,
        ByteOrder.nativeOrder()
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
    Assert.assertEquals(ImmutableSet.of(ColumnType.LONG), column.getColumnTypes(xPath));
    Assert.assertEquals(ColumnType.LONG, column.getColumnHolder(xPath).getCapabilities().toColumnType());
    ColumnValueSelector<?> xSelector = column.makeColumnValueSelector(xPath, offset);
    DimensionSelector xDimSelector = column.makeDimensionSelector(xPath, offset, null);
    ColumnIndexSupplier xIndexSupplier = column.getColumnIndexSupplier(xPath);
    Assert.assertNotNull(xIndexSupplier);
    StringValueSetIndexes xValueIndex = xIndexSupplier.as(StringValueSetIndexes.class);
    DruidPredicateIndexes xPredicateIndex = xIndexSupplier.as(DruidPredicateIndexes.class);
    NullValueIndex xNulls = xIndexSupplier.as(NullValueIndex.class);

    final List<NestedPathPart> yPath = NestedPathFinder.parseJsonPath("$.y");
    Assert.assertEquals(ImmutableSet.of(ColumnType.DOUBLE), column.getColumnTypes(yPath));
    Assert.assertEquals(ColumnType.DOUBLE, column.getColumnHolder(yPath).getCapabilities().toColumnType());
    ColumnValueSelector<?> ySelector = column.makeColumnValueSelector(yPath, offset);
    DimensionSelector yDimSelector = column.makeDimensionSelector(yPath, offset, null);
    ColumnIndexSupplier yIndexSupplier = column.getColumnIndexSupplier(yPath);
    Assert.assertNotNull(yIndexSupplier);
    StringValueSetIndexes yValueIndex = yIndexSupplier.as(StringValueSetIndexes.class);
    DruidPredicateIndexes yPredicateIndex = yIndexSupplier.as(DruidPredicateIndexes.class);
    NullValueIndex yNulls = yIndexSupplier.as(NullValueIndex.class);

    final List<NestedPathPart> zPath = NestedPathFinder.parseJsonPath("$.z");
    Assert.assertEquals(ImmutableSet.of(ColumnType.STRING), column.getColumnTypes(zPath));
    Assert.assertEquals(ColumnType.STRING, column.getColumnHolder(zPath).getCapabilities().toColumnType());
    ColumnValueSelector<?> zSelector = column.makeColumnValueSelector(zPath, offset);
    DimensionSelector zDimSelector = column.makeDimensionSelector(zPath, offset, null);
    ColumnIndexSupplier zIndexSupplier = column.getColumnIndexSupplier(zPath);
    Assert.assertNotNull(zIndexSupplier);
    StringValueSetIndexes zValueIndex = zIndexSupplier.as(StringValueSetIndexes.class);
    DruidPredicateIndexes zPredicateIndex = zIndexSupplier.as(DruidPredicateIndexes.class);
    NullValueIndex zNulls = zIndexSupplier.as(NullValueIndex.class);

    final List<NestedPathPart> vPath = NestedPathFinder.parseJsonPath("$.v");
    Assert.assertEquals(
        ImmutableSet.of(ColumnType.STRING, ColumnType.LONG, ColumnType.DOUBLE),
        column.getColumnTypes(vPath)
    );
    Assert.assertEquals(ColumnType.STRING, column.getColumnHolder(vPath).getCapabilities().toColumnType());
    ColumnValueSelector<?> vSelector = column.makeColumnValueSelector(vPath, offset);
    DimensionSelector vDimSelector = column.makeDimensionSelector(vPath, offset, null);
    ColumnIndexSupplier vIndexSupplier = column.getColumnIndexSupplier(vPath);
    Assert.assertNotNull(vIndexSupplier);
    StringValueSetIndexes vValueIndex = vIndexSupplier.as(StringValueSetIndexes.class);
    DruidPredicateIndexes vPredicateIndex = vIndexSupplier.as(DruidPredicateIndexes.class);
    NullValueIndex vNulls = vIndexSupplier.as(NullValueIndex.class);

    final List<NestedPathPart> nullishPath = NestedPathFinder.parseJsonPath("$.nullish");
    Assert.assertEquals(ImmutableSet.of(ColumnType.STRING), column.getColumnTypes(nullishPath));
    Assert.assertEquals(ColumnType.STRING, column.getColumnHolder(nullishPath).getCapabilities().toColumnType());
    ColumnValueSelector<?> nullishSelector = column.makeColumnValueSelector(nullishPath, offset);
    DimensionSelector nullishDimSelector = column.makeDimensionSelector(nullishPath, offset, null);
    ColumnIndexSupplier nullishIndexSupplier = column.getColumnIndexSupplier(nullishPath);
    Assert.assertNotNull(nullishIndexSupplier);
    StringValueSetIndexes nullishValueIndex = nullishIndexSupplier.as(StringValueSetIndexes.class);
    DruidPredicateIndexes nullishPredicateIndex = nullishIndexSupplier.as(DruidPredicateIndexes.class);
    NullValueIndex nullishNulls = nullishIndexSupplier.as(NullValueIndex.class);

    Assert.assertEquals(ImmutableList.of(nullishPath, vPath, xPath, yPath, zPath), column.getNestedFields());

    for (int i = 0; i < data.size(); i++) {
      Map row = data.get(i);
      Assert.assertEquals(
          JSON_MAPPER.writeValueAsString(row),
          JSON_MAPPER.writeValueAsString(StructuredData.unwrap(rawSelector.getObject()))
      );

      testPath(row, i, "v", vSelector, vDimSelector, vValueIndex, vPredicateIndex, vNulls, null);
      testPath(row, i, "x", xSelector, xDimSelector, xValueIndex, xPredicateIndex, xNulls, ColumnType.LONG);
      testPath(row, i, "y", ySelector, yDimSelector, yValueIndex, yPredicateIndex, yNulls, ColumnType.DOUBLE);
      testPath(row, i, "z", zSelector, zDimSelector, zValueIndex, zPredicateIndex, zNulls, ColumnType.STRING);
      testPath(
          row,
          i,
          "nullish",
          nullishSelector,
          nullishDimSelector,
          nullishValueIndex,
          nullishPredicateIndex,
          nullishNulls,
          ColumnType.STRING
      );

      offset.increment();
    }
  }

  private void smokeTestArrays(NestedDataComplexColumn column) throws IOException
  {
    SimpleAscendingOffset offset = new SimpleAscendingOffset(arrayTestData.size());
    NoFilterVectorOffset vectorOffset = new NoFilterVectorOffset(4, 0, arrayTestData.size());
    WrappedRoaringBitmap bitmap = new WrappedRoaringBitmap();
    for (int i = 0; i < arrayTestData.size(); i++) {
      if (i % 2 == 0) {
        bitmap.add(i);
      }
    }
    BitmapVectorOffset bitmapVectorOffset = new BitmapVectorOffset(
        4,
        bitmap.toImmutableBitmap(),
        0,
        arrayTestData.size()
    );

    ColumnValueSelector<?> rawSelector = column.makeColumnValueSelector(offset);
    VectorObjectSelector rawVectorSelector = column.makeVectorObjectSelector(vectorOffset);
    VectorObjectSelector rawVectorSelectorFiltered = column.makeVectorObjectSelector(bitmapVectorOffset);

    final List<NestedPathPart> sPath = NestedPathFinder.parseJsonPath("$.s");
    Assert.assertEquals(ImmutableSet.of(ColumnType.STRING_ARRAY), column.getColumnTypes(sPath));
    Assert.assertEquals(ColumnType.STRING_ARRAY, column.getColumnHolder(sPath).getCapabilities().toColumnType());
    ColumnValueSelector<?> sSelector = column.makeColumnValueSelector(sPath, offset);
    VectorObjectSelector sVectorSelector = column.makeVectorObjectSelector(sPath, vectorOffset);
    VectorObjectSelector sVectorSelectorFiltered = column.makeVectorObjectSelector(sPath, bitmapVectorOffset);
    ColumnIndexSupplier sIndexSupplier = column.getColumnIndexSupplier(sPath);
    Assert.assertNotNull(sIndexSupplier);
    Assert.assertNull(sIndexSupplier.as(StringValueSetIndexes.class));
    Assert.assertNull(sIndexSupplier.as(DruidPredicateIndexes.class));
    NullValueIndex sNulls = sIndexSupplier.as(NullValueIndex.class);

    final List<NestedPathPart> sElementPath = NestedPathFinder.parseJsonPath("$.s[1]");
    ColumnValueSelector<?> sElementSelector = column.makeColumnValueSelector(sElementPath, offset);
    VectorObjectSelector sElementVectorSelector = column.makeVectorObjectSelector(sElementPath, vectorOffset);
    VectorObjectSelector sElementFilteredVectorSelector = column.makeVectorObjectSelector(
        sElementPath,
        bitmapVectorOffset
    );
    ColumnIndexSupplier sElementIndexSupplier = column.getColumnIndexSupplier(sElementPath);
    Assert.assertNotNull(sElementIndexSupplier);
    Assert.assertNull(sElementIndexSupplier.as(StringValueSetIndexes.class));
    Assert.assertNull(sElementIndexSupplier.as(DruidPredicateIndexes.class));
    Assert.assertNull(sElementIndexSupplier.as(NullValueIndex.class));

    final List<NestedPathPart> lPath = NestedPathFinder.parseJsonPath("$.l");
    Assert.assertEquals(ImmutableSet.of(ColumnType.LONG_ARRAY), column.getColumnTypes(lPath));
    Assert.assertEquals(ColumnType.LONG_ARRAY, column.getColumnHolder(lPath).getCapabilities().toColumnType());
    ColumnValueSelector<?> lSelector = column.makeColumnValueSelector(lPath, offset);
    VectorObjectSelector lVectorSelector = column.makeVectorObjectSelector(lPath, vectorOffset);
    VectorObjectSelector lVectorSelectorFiltered = column.makeVectorObjectSelector(lPath, bitmapVectorOffset);
    ColumnIndexSupplier lIndexSupplier = column.getColumnIndexSupplier(lPath);
    Assert.assertNotNull(lIndexSupplier);
    Assert.assertNull(lIndexSupplier.as(StringValueSetIndexes.class));
    Assert.assertNull(lIndexSupplier.as(DruidPredicateIndexes.class));
    NullValueIndex lNulls = lIndexSupplier.as(NullValueIndex.class);

    final List<NestedPathPart> lElementPath = NestedPathFinder.parseJsonPath("$.l[1]");
    ColumnValueSelector<?> lElementSelector = column.makeColumnValueSelector(lElementPath, offset);
    VectorValueSelector lElementVectorSelector = column.makeVectorValueSelector(lElementPath, vectorOffset);
    VectorObjectSelector lElementVectorObjectSelector = column.makeVectorObjectSelector(lElementPath, vectorOffset);
    VectorValueSelector lElementFilteredVectorSelector = column.makeVectorValueSelector(
        lElementPath,
        bitmapVectorOffset
    );
    ColumnIndexSupplier lElementIndexSupplier = column.getColumnIndexSupplier(lElementPath);
    Assert.assertNotNull(lElementIndexSupplier);
    Assert.assertNull(lElementIndexSupplier.as(StringValueSetIndexes.class));
    Assert.assertNull(lElementIndexSupplier.as(DruidPredicateIndexes.class));
    Assert.assertNull(lElementIndexSupplier.as(NullValueIndex.class));

    final List<NestedPathPart> dPath = NestedPathFinder.parseJsonPath("$.d");
    Assert.assertEquals(ImmutableSet.of(ColumnType.DOUBLE_ARRAY), column.getColumnTypes(dPath));
    Assert.assertEquals(ColumnType.DOUBLE_ARRAY, column.getColumnHolder(dPath).getCapabilities().toColumnType());
    ColumnValueSelector<?> dSelector = column.makeColumnValueSelector(dPath, offset);
    VectorObjectSelector dVectorSelector = column.makeVectorObjectSelector(dPath, vectorOffset);
    VectorObjectSelector dVectorSelectorFiltered = column.makeVectorObjectSelector(dPath, bitmapVectorOffset);
    ColumnIndexSupplier dIndexSupplier = column.getColumnIndexSupplier(dPath);
    Assert.assertNotNull(dIndexSupplier);
    Assert.assertNull(dIndexSupplier.as(StringValueSetIndexes.class));
    Assert.assertNull(dIndexSupplier.as(DruidPredicateIndexes.class));
    NullValueIndex dNulls = dIndexSupplier.as(NullValueIndex.class);

    final List<NestedPathPart> dElementPath = NestedPathFinder.parseJsonPath("$.d[1]");
    ColumnValueSelector<?> dElementSelector = column.makeColumnValueSelector(dElementPath, offset);
    VectorValueSelector dElementVectorSelector = column.makeVectorValueSelector(dElementPath, vectorOffset);
    VectorObjectSelector dElementVectorObjectSelector = column.makeVectorObjectSelector(dElementPath, vectorOffset);
    VectorValueSelector dElementFilteredVectorSelector = column.makeVectorValueSelector(
        dElementPath,
        bitmapVectorOffset
    );
    ColumnIndexSupplier dElementIndexSupplier = column.getColumnIndexSupplier(dElementPath);
    Assert.assertNotNull(dElementIndexSupplier);
    Assert.assertNull(dElementIndexSupplier.as(StringValueSetIndexes.class));
    Assert.assertNull(dElementIndexSupplier.as(DruidPredicateIndexes.class));
    Assert.assertNull(dElementIndexSupplier.as(NullValueIndex.class));


    ImmutableBitmap sNullIndex = sNulls.get().computeBitmapResult(resultFactory, false);
    ImmutableBitmap lNullIndex = lNulls.get().computeBitmapResult(resultFactory, false);
    ImmutableBitmap dNullIndex = dNulls.get().computeBitmapResult(resultFactory, false);

    int rowCounter = 0;
    while (offset.withinBounds()) {
      Map row = arrayTestData.get(rowCounter);
      Assert.assertEquals(
          JSON_MAPPER.writeValueAsString(row),
          JSON_MAPPER.writeValueAsString(StructuredData.unwrap(rawSelector.getObject()))
      );

      Object[] s = (Object[]) row.get("s");
      Object[] l = (Object[]) row.get("l");
      Object[] d = (Object[]) row.get("d");
      Assert.assertArrayEquals(s, (Object[]) sSelector.getObject());
      Assert.assertArrayEquals(l, (Object[]) lSelector.getObject());
      Assert.assertArrayEquals(d, (Object[]) dSelector.getObject());
      Assert.assertEquals(s == null, sNullIndex.get(rowCounter));
      Assert.assertEquals(l == null, lNullIndex.get(rowCounter));
      Assert.assertEquals(d == null, dNullIndex.get(rowCounter));

      if (s == null || s.length < 1) {
        Assert.assertNull(sElementSelector.getObject());
      } else {
        Assert.assertEquals(s[1], sElementSelector.getObject());
      }
      if (l == null || l.length < 1 || l[1] == null) {
        Assert.assertTrue(lElementSelector.isNull());
        Assert.assertNull(lElementSelector.getObject());
      } else {
        Assert.assertEquals(l[1], lElementSelector.getLong());
        Assert.assertEquals(l[1], lElementSelector.getObject());
      }
      if (d == null || d.length < 1 || d[1] == null) {
        Assert.assertTrue(dElementSelector.isNull());
        Assert.assertNull(dElementSelector.getObject());
      } else {
        Assert.assertEquals((Double) d[1], dElementSelector.getDouble(), 0.0);
        Assert.assertEquals(d[1], dElementSelector.getObject());
      }

      offset.increment();
      rowCounter++;
    }

    rowCounter = 0;
    while (!vectorOffset.isDone()) {
      final Object[] rawVector = rawVectorSelector.getObjectVector();
      final Object[] sVector = sVectorSelector.getObjectVector();
      final Object[] lVector = lVectorSelector.getObjectVector();
      final Object[] dVector = dVectorSelector.getObjectVector();
      final Object[] sElementVector = sElementVectorSelector.getObjectVector();
      final long[] lElementVector = lElementVectorSelector.getLongVector();
      final boolean[] lElementNulls = lElementVectorSelector.getNullVector();
      final Object[] lElementObjectVector = lElementVectorObjectSelector.getObjectVector();
      final double[] dElementVector = dElementVectorSelector.getDoubleVector();
      final boolean[] dElementNulls = dElementVectorSelector.getNullVector();
      final Object[] dElementObjectVector = dElementVectorObjectSelector.getObjectVector();

      for (int i = 0; i < vectorOffset.getCurrentVectorSize(); i++, rowCounter++) {

        Map row = arrayTestData.get(rowCounter);
        Assert.assertEquals(
            JSON_MAPPER.writeValueAsString(row),
            JSON_MAPPER.writeValueAsString(StructuredData.unwrap(rawVector[i]))
        );
        Object[] s = (Object[]) row.get("s");
        Object[] l = (Object[]) row.get("l");
        Object[] d = (Object[]) row.get("d");

        Assert.assertArrayEquals(s, (Object[]) sVector[i]);
        Assert.assertArrayEquals(l, (Object[]) lVector[i]);
        Assert.assertArrayEquals(d, (Object[]) dVector[i]);

        if (s == null || s.length < 1) {
          Assert.assertNull(sElementVector[i]);
        } else {
          Assert.assertEquals(s[1], sElementVector[i]);
        }
        if (l == null || l.length < 1 || l[1] == null) {
          Assert.assertTrue(lElementNulls[i]);
          Assert.assertNull(lElementObjectVector[i]);
        } else {
          Assert.assertEquals(l[1], lElementVector[i]);
          Assert.assertEquals(l[1], lElementObjectVector[i]);
        }
        if (d == null || d.length < 1 || d[1] == null) {
          Assert.assertTrue(dElementNulls[i]);
          Assert.assertNull(dElementObjectVector[i]);
        } else {
          Assert.assertEquals((Double) d[1], dElementVector[i], 0.0);
          Assert.assertEquals(d[1], dElementObjectVector[i]);
        }
      }
      vectorOffset.advance();
    }

    rowCounter = 0;
    while (!bitmapVectorOffset.isDone()) {
      final Object[] rawVector = rawVectorSelectorFiltered.getObjectVector();
      final Object[] sVector = sVectorSelectorFiltered.getObjectVector();
      final Object[] lVector = lVectorSelectorFiltered.getObjectVector();
      final Object[] dVector = dVectorSelectorFiltered.getObjectVector();
      final Object[] sElementVector = sElementFilteredVectorSelector.getObjectVector();
      final long[] lElementVector = lElementFilteredVectorSelector.getLongVector();
      final boolean[] lElementNulls = lElementFilteredVectorSelector.getNullVector();
      final double[] dElementVector = dElementFilteredVectorSelector.getDoubleVector();
      final boolean[] dElementNulls = dElementFilteredVectorSelector.getNullVector();

      for (int i = 0; i < bitmapVectorOffset.getCurrentVectorSize(); i++, rowCounter += 2) {
        Map row = arrayTestData.get(rowCounter);
        Assert.assertEquals(
            JSON_MAPPER.writeValueAsString(row),
            JSON_MAPPER.writeValueAsString(StructuredData.unwrap(rawVector[i]))
        );
        Object[] s = (Object[]) row.get("s");
        Object[] l = (Object[]) row.get("l");
        Object[] d = (Object[]) row.get("d");

        Assert.assertArrayEquals(s, (Object[]) sVector[i]);
        Assert.assertArrayEquals(l, (Object[]) lVector[i]);
        Assert.assertArrayEquals(d, (Object[]) dVector[i]);

        if (s == null || s.length < 1) {
          Assert.assertNull(sElementVector[i]);
        } else {
          Assert.assertEquals(s[1], sElementVector[i]);
        }
        if (l == null || l.length < 1 || l[1] == null) {
          Assert.assertTrue(lElementNulls[i]);
        } else {
          Assert.assertEquals(l[1], lElementVector[i]);
        }
        if (d == null || d.length < 1 || d[1] == null) {
          Assert.assertTrue(dElementNulls[i]);
        } else {
          Assert.assertEquals((Double) d[1], dElementVector[i], 0.0);
        }
      }
      bitmapVectorOffset.advance();
    }
  }

  private void testPath(
      Map row,
      int rowNumber,
      String path,
      ColumnValueSelector<?> valueSelector,
      DimensionSelector dimSelector,
      StringValueSetIndexes valueSetIndex,
      DruidPredicateIndexes predicateIndex,
      NullValueIndex nullValueIndex,
      @Nullable ColumnType singleType
  )
  {
    final Object inputValue = row.get(path);
    // in default value mode, even though the input row had an empty string, the selector spits out null, so we want
    // to take the null checking path
    final boolean isStringAndNullEquivalent =
        inputValue instanceof String && NullHandling.isNullOrEquivalent((String) inputValue);

    if (row.containsKey(path) && inputValue != null && !isStringAndNullEquivalent) {
      Assert.assertEquals(inputValue, valueSelector.getObject());
      if (ColumnType.LONG.equals(singleType)) {
        Assert.assertEquals(inputValue, valueSelector.getLong());
        Assert.assertFalse(path + " is not null", valueSelector.isNull());
      } else if (ColumnType.DOUBLE.equals(singleType)) {
        Assert.assertEquals((double) inputValue, valueSelector.getDouble(), 0.0);
        Assert.assertFalse(path + " is not null", valueSelector.isNull());
      }

      final String theString = String.valueOf(inputValue);
      Assert.assertEquals(theString, dimSelector.getObject());
      String dimSelectorLookupVal = dimSelector.lookupName(dimSelector.getRow().get(0));
      Assert.assertEquals(theString, dimSelectorLookupVal);
      Assert.assertEquals(dimSelector.idLookup().lookupId(dimSelectorLookupVal), dimSelector.getRow().get(0));

      Assert.assertTrue(valueSetIndex.forValue(theString).computeBitmapResult(resultFactory, false).get(rowNumber));
      Assert.assertTrue(valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of(theString)))
                                     .computeBitmapResult(resultFactory, false)
                                     .get(rowNumber));
      Assert.assertTrue(predicateIndex.forPredicate(new SelectorPredicateFactory(theString))
                                      .computeBitmapResult(resultFactory, false)
                                      .get(rowNumber));
      Assert.assertFalse(valueSetIndex.forValue(NO_MATCH).computeBitmapResult(resultFactory, false).get(rowNumber));
      Assert.assertFalse(valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of(NO_MATCH)))
                                      .computeBitmapResult(resultFactory, false)
                                      .get(rowNumber));
      Assert.assertFalse(predicateIndex.forPredicate(new SelectorPredicateFactory(NO_MATCH))
                                       .computeBitmapResult(resultFactory, false)
                                       .get(rowNumber));
      Assert.assertFalse(nullValueIndex.get().computeBitmapResult(resultFactory, false).get(rowNumber));

      Assert.assertTrue(dimSelector.makeValueMatcher(theString).matches(false));
      Assert.assertFalse(dimSelector.makeValueMatcher(NO_MATCH).matches(false));
      Assert.assertTrue(dimSelector.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo(theString)).matches(false));
      Assert.assertFalse(dimSelector.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo(NO_MATCH)).matches(false));
    } else {
      Assert.assertNull(valueSelector.getObject());
      Assert.assertTrue(path, valueSelector.isNull());

      Assert.assertEquals(0, dimSelector.getRow().get(0));
      Assert.assertNull(dimSelector.getObject());
      Assert.assertNull(dimSelector.lookupName(dimSelector.getRow().get(0)));

      Assert.assertTrue(nullValueIndex.get().computeBitmapResult(resultFactory, false).get(rowNumber));
      Assert.assertTrue(valueSetIndex.forValue(null).computeBitmapResult(resultFactory, false).get(rowNumber));
      Assert.assertTrue(predicateIndex.forPredicate(new SelectorPredicateFactory(null))
                                      .computeBitmapResult(resultFactory, false)
                                      .get(rowNumber));
      Assert.assertFalse(valueSetIndex.forValue(NO_MATCH).computeBitmapResult(resultFactory, false).get(rowNumber));


      Assert.assertFalse(valueSetIndex.forValue(NO_MATCH).computeBitmapResult(resultFactory, false).get(rowNumber));
      Assert.assertFalse(predicateIndex.forPredicate(new SelectorPredicateFactory(NO_MATCH))
                                       .computeBitmapResult(resultFactory, false)
                                       .get(rowNumber));

      Assert.assertTrue(dimSelector.makeValueMatcher((String) null).matches(false));
      Assert.assertFalse(dimSelector.makeValueMatcher(NO_MATCH).matches(false));
      Assert.assertTrue(dimSelector.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo(null)).matches(false));
      Assert.assertFalse(dimSelector.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo(NO_MATCH)).matches(false));
    }
  }

  static class SettableSelector extends ObjectColumnSelector<StructuredData>
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
}
