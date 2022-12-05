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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.DirectQueryProcessingPool;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.DictionaryEncodedStringValueIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;

public class DumpSegmentTest extends InitializedNullHandlingTest
{
  private final AggregationTestHelper helper;
  private final Closer closer;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public DumpSegmentTest()
  {
    NestedDataModule.registerHandlersAndSerde();
    List<? extends Module> mods = NestedDataModule.getJacksonModulesList();
    this.helper = AggregationTestHelper.createScanQueryAggregationTestHelper(
        mods,
        tempFolder
    );
    this.closer = Closer.create();
  }

  @After
  public void teardown() throws IOException
  {
    closer.close();
  }

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
        tempFolder.newFile().getPath(),
        queryableIndex,
        ImmutableList.of("x", "y"),
        false
    );
    Assert.assertTrue(true);
  }

  @Test
  public void testDumpNestedColumn() throws Exception
  {
    Injector injector = Mockito.mock(Injector.class);
    ObjectMapper mapper = TestHelper.makeJsonMapper();
    mapper.registerModules(NestedDataModule.getJacksonModulesList());
    Mockito.when(injector.getInstance(Key.get(ObjectMapper.class, Json.class))).thenReturn(mapper);

    List<Segment> segments = createSegments(helper, tempFolder, closer);
    QueryableIndex queryableIndex = segments.get(0).asQueryableIndex();

    File outputFile = tempFolder.newFile();
    DumpSegment.runDumpNestedColumn(
        injector,
        outputFile.getPath(),
        queryableIndex,
        "nest"
    );
    final byte[] fileBytes = Files.readAllBytes(outputFile.toPath());
    final String output = StringUtils.fromUtf8(fileBytes);
    Assert.assertEquals("{\"nest\":{\"fields\":[{\"path\":\"$.x\",\"types\":[\"LONG\"]},{\"path\":\"$.y\",\"types\":[\"DOUBLE\"]},{\"path\":\"$.z\",\"types\":[\"STRING\"]}],\"dictionaries\":{\"strings\":[{\"globalId\":0,\"value\":null},{\"globalId\":1,\"value\":\"a\"},{\"globalId\":2,\"value\":\"b\"}],\"longs\":[{\"globalId\":3,\"value\":100},{\"globalId\":4,\"value\":200},{\"globalId\":5,\"value\":400}],\"doubles\":[{\"globalId\":6,\"value\":1.1},{\"globalId\":7,\"value\":2.2},{\"globalId\":8,\"value\":3.3}],\"nullRows\":[]}}}", output);
  }

  @Test
  public void testDumpNestedColumnPath() throws Exception
  {
    Injector injector = Mockito.mock(Injector.class);
    ObjectMapper mapper = TestHelper.makeJsonMapper();
    mapper.registerModules(NestedDataModule.getJacksonModulesList());
    Mockito.when(injector.getInstance(Key.get(ObjectMapper.class, Json.class))).thenReturn(mapper);
    List<Segment> segments = createSegments(helper, tempFolder, closer);
    QueryableIndex queryableIndex = segments.get(0).asQueryableIndex();

    File outputFile = tempFolder.newFile();
    DumpSegment.runDumpNestedColumnPath(
        injector,
        outputFile.getPath(),
        queryableIndex,
        "nest",
        "$.x"
    );
    final byte[] fileBytes = Files.readAllBytes(outputFile.toPath());
    final String output = StringUtils.fromUtf8(fileBytes);
    Assert.assertEquals("{\"bitmapSerdeFactory\":{\"type\":\"roaring\",\"compressRunOnSerialization\":true},\"nest\":{\"$.x\":{\"types\":[\"LONG\"],\"dictionary\":[{\"localId\":0,\"globalId\":0,\"value\":null,\"rows\":[4]},{\"localId\":1,\"globalId\":3,\"value\":\"100\",\"rows\":[3]},{\"localId\":2,\"globalId\":4,\"value\":\"200\",\"rows\":[0,2]},{\"localId\":3,\"globalId\":5,\"value\":\"400\",\"rows\":[1]}],\"column\":[{\"row\":0,\"raw\":{\"x\":200,\"y\":2.2},\"fieldId\":2,\"fieldValue\":\"200\"},{\"row\":1,\"raw\":{\"x\":400,\"y\":1.1,\"z\":\"a\"},\"fieldId\":3,\"fieldValue\":\"400\"},{\"row\":2,\"raw\":{\"x\":200,\"z\":\"b\"},\"fieldId\":2,\"fieldValue\":\"200\"},{\"row\":3,\"raw\":{\"x\":100,\"y\":1.1,\"z\":\"a\"},\"fieldId\":1,\"fieldValue\":\"100\"},{\"row\":4,\"raw\":{\"y\":3.3,\"z\":\"b\"},\"fieldId\":0,\"fieldValue\":null}]}}}", output);
  }

  public static List<Segment> createSegments(
      AggregationTestHelper helper,
      TemporaryFolder tempFolder,
      Closer closer
  ) throws Exception
  {
    File segmentDir = tempFolder.newFolder();
    File inputFile = readFileFromClasspath("nested-test-data.json");
    FileInputStream inputDataStream = new FileInputStream(inputFile);
    String parserJson = readFileFromClasspathAsString("nested-test-parser.json");
    String aggJson = readFileFromClasspathAsString("nested-test-aggs.json");

    helper.createIndex(
        inputDataStream,
        parserJson,
        aggJson,
        segmentDir,
        0,
        Granularities.HOUR,
        1000,
        true
    );

    final List<Segment> segments = Lists.transform(
        ImmutableList.of(segmentDir),
        dir -> {
          try {
            return closer.register(new QueryableIndexSegment(helper.getIndexIO().loadIndex(dir), SegmentId.dummy("")));
          }
          catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        }
    );

    return segments;
  }
  public static File readFileFromClasspath(String fileName)
  {
    return new File(NestedDataTestUtils.class.getClassLoader().getResource(fileName).getFile());
  }

  public static String readFileFromClasspathAsString(String fileName) throws IOException
  {
    return com.google.common.io.Files.asCharSource(readFileFromClasspath(fileName), StandardCharsets.UTF_8).read();
  }
}
