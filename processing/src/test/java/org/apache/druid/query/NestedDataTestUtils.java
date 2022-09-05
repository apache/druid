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

package org.apache.druid.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.nary.TrinaryFn;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.timeline.SegmentId;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class NestedDataTestUtils
{
  public static final String SIMPLE_DATA_FILE = "simple-nested-test-data.json";
  public static final String SIMPLE_PARSER_FILE = "simple-nested-test-data-parser.json";
  public static final String SIMPLE_DATA_TSV_FILE = "simple-nested-test-data.tsv";
  public static final String SIMPLE_PARSER_TSV_FILE = "simple-nested-test-data-tsv-parser.json";
  public static final String SIMPLE_PARSER_TSV_TRANSFORM_FILE = "simple-nested-test-data-tsv-transform.json";
  public static final String SIMPLE_AGG_FILE = "simple-nested-test-data-aggs.json";

  public static final String NUMERIC_DATA_FILE = "numeric-nested-test-data.json";
  public static final String NUMERIC_PARSER_FILE = "numeric-nested-test-data-parser.json";

  public static final ObjectMapper JSON_MAPPER;

  static {
    JSON_MAPPER = TestHelper.makeJsonMapper();
    JSON_MAPPER.registerModules(NestedDataModule.getJacksonModulesList());
  }

  public static List<Segment> createSegments(
      AggregationTestHelper helper,
      TemporaryFolder tempFolder,
      Closer closer,
      Granularity granularity,
      boolean rollup,
      int maxRowCount
  ) throws Exception
  {
    return createSegments(
        helper,
        tempFolder,
        closer,
        SIMPLE_DATA_FILE,
        SIMPLE_PARSER_FILE,
        SIMPLE_AGG_FILE,
        granularity,
        rollup,
        maxRowCount
    );
  }

  public static List<Segment> createTsvSegments(
      AggregationTestHelper helper,
      TemporaryFolder tempFolder,
      Closer closer,
      Granularity granularity,
      boolean rollup,
      int maxRowCount
  ) throws Exception
  {
    return createSegments(
        helper,
        tempFolder,
        closer,
        SIMPLE_DATA_TSV_FILE,
        SIMPLE_PARSER_TSV_FILE,
        SIMPLE_PARSER_TSV_TRANSFORM_FILE,
        SIMPLE_AGG_FILE,
        granularity,
        rollup,
        maxRowCount
    );
  }

  public static Segment createIncrementalIndex(
      Granularity granularity,
      boolean rollup,
      boolean deserializeComplexMetrics,
      int maxRowCount
  )
      throws Exception
  {
    return createIncrementalIndex(
        SIMPLE_DATA_FILE,
        SIMPLE_PARSER_FILE,
        SIMPLE_AGG_FILE,
        granularity,
        rollup,
        deserializeComplexMetrics,
        maxRowCount
    );
  }

  public static List<Segment> createSegments(
      AggregationTestHelper helper,
      TemporaryFolder tempFolder,
      Closer closer,
      String inputFileName,
      String parserJsonFileName,
      String aggJsonFileName,
      Granularity granularity,
      boolean rollup,
      int maxRowCount
  ) throws Exception
  {
    File segmentDir = tempFolder.newFolder();
    File inputFile = readFileFromClasspath(inputFileName);
    FileInputStream inputDataStream = new FileInputStream(inputFile);
    String parserJson = readFileFromClasspathAsString(parserJsonFileName);
    String aggJson = readFileFromClasspathAsString(aggJsonFileName);

    helper.createIndex(
        inputDataStream,
        parserJson,
        aggJson,
        segmentDir,
        0,
        granularity,
        maxRowCount,
        rollup
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

  public static List<Segment> createSegments(
      AggregationTestHelper helper,
      TemporaryFolder tempFolder,
      Closer closer,
      String inputFileName,
      String parserJsonFileName,
      String transformSpecJsonFileName,
      String aggJsonFileName,
      Granularity granularity,
      boolean rollup,
      int maxRowCount
  ) throws Exception
  {
    File segmentDir = tempFolder.newFolder();
    File inputFile = readFileFromClasspath(inputFileName);
    FileInputStream inputDataStream = new FileInputStream(inputFile);
    String parserJson = readFileFromClasspathAsString(parserJsonFileName);
    String transformSpecJson = readFileFromClasspathAsString(transformSpecJsonFileName);
    String aggJson = readFileFromClasspathAsString(aggJsonFileName);

    helper.createIndex(
        inputDataStream,
        parserJson,
        transformSpecJson,
        aggJson,
        segmentDir,
        0,
        granularity,
        maxRowCount,
        rollup
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

  public static Segment createIncrementalIndex(
      String inputFileName,
      String parserJsonFileName,
      String aggJsonFileName,
      Granularity granularity,
      boolean rollup,
      boolean deserializeComplexMetrics,
      int maxRowCount
  )
      throws Exception
  {
    File inputFile = readFileFromClasspath(inputFileName);
    FileInputStream inputDataStream = new FileInputStream(inputFile);
    String parserJson = readFileFromClasspathAsString(parserJsonFileName);
    String aggJson = readFileFromClasspathAsString(aggJsonFileName);
    StringInputRowParser parser = JSON_MAPPER.readValue(parserJson, StringInputRowParser.class);

    LineIterator iter = IOUtils.lineIterator(inputDataStream, "UTF-8");
    List<AggregatorFactory> aggregatorSpecs = JSON_MAPPER.readValue(
        aggJson,
        new TypeReference<List<AggregatorFactory>>()
        {
        }
    );
    IncrementalIndex index = AggregationTestHelper.createIncrementalIndex(
        iter,
        parser,
        parser.getParseSpec().getDimensionsSpec().getDimensions(),
        aggregatorSpecs.toArray(new AggregatorFactory[0]),
        0,
        granularity,
        deserializeComplexMetrics,
        maxRowCount,
        rollup
    );
    return new IncrementalIndexSegment(index, SegmentId.dummy("test_datasource"));
  }

  public static Segment createDefaultHourlyIncrementalIndex() throws Exception
  {
    return createIncrementalIndex(Granularities.HOUR, true, true, 1000);
  }

  public static Segment createDefaultDailyIncrementalIndex() throws Exception
  {
    return createIncrementalIndex(Granularities.DAY, true, true, 1000);
  }

  public static List<Segment> createDefaultHourlySegments(
      AggregationTestHelper helper,
      TemporaryFolder tempFolder,
      Closer closer
  )
      throws Exception
  {
    return createSegments(
        helper,
        tempFolder,
        closer,
        Granularities.HOUR,
        true,
        1000
    );
  }

  public static List<Segment> createDefaultHourlySegmentsTsv(
      AggregationTestHelper helper,
      TemporaryFolder tempFolder,
      Closer closer
  )
      throws Exception
  {
    return createTsvSegments(
        helper,
        tempFolder,
        closer,
        Granularities.HOUR,
        true,
        1000
    );
  }

  public static List<Segment> createDefaultDaySegments(
      AggregationTestHelper helper,
      TemporaryFolder tempFolder,
      Closer closer
  )
      throws Exception
  {
    return createSegments(
        helper,
        tempFolder,
        closer,
        Granularities.DAY,
        true,
        1000
    );
  }

  public static File readFileFromClasspath(String fileName)
  {
    return new File(NestedDataTestUtils.class.getClassLoader().getResource(fileName).getFile());
  }

  public static String readFileFromClasspathAsString(String fileName) throws IOException
  {
    return Files.asCharSource(readFileFromClasspath(fileName), StandardCharsets.UTF_8).read();
  }

  public static List<TrinaryFn<AggregationTestHelper, TemporaryFolder, Closer, List<Segment>>> getSegmentGenerators()
  {
    final List<TrinaryFn<AggregationTestHelper, TemporaryFolder, Closer, List<Segment>>> segmentsGenerators = new ArrayList<>();
    segmentsGenerators.add(new TrinaryFn<AggregationTestHelper, TemporaryFolder, Closer, List<Segment>>()
    {
      @Override
      public List<Segment> apply(AggregationTestHelper helper, TemporaryFolder tempFolder, Closer closer)
      {
        try {
          return ImmutableList.<Segment>builder()
                              .addAll(NestedDataTestUtils.createDefaultHourlySegments(helper, tempFolder, closer))
                              .add(NestedDataTestUtils.createDefaultHourlyIncrementalIndex())
                              .build();
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public String toString()
      {
        return "mixed";
      }
    });
    segmentsGenerators.add(new TrinaryFn<AggregationTestHelper, TemporaryFolder, Closer, List<Segment>>()
    {
      @Override
      public List<Segment> apply(AggregationTestHelper helper, TemporaryFolder tempFolder, Closer closer)
      {
        try {
          return ImmutableList.of(
              NestedDataTestUtils.createDefaultHourlyIncrementalIndex(),
              NestedDataTestUtils.createDefaultHourlyIncrementalIndex()
          );
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public String toString()
      {
        return "incremental";
      }
    });
    segmentsGenerators.add(new TrinaryFn<AggregationTestHelper, TemporaryFolder, Closer, List<Segment>>()
    {
      @Override
      public List<Segment> apply(AggregationTestHelper helper, TemporaryFolder tempFolder, Closer closer)
      {
        try {
          return ImmutableList.<Segment>builder()
                              .addAll(NestedDataTestUtils.createDefaultHourlySegments(helper, tempFolder, closer))
                              .addAll(NestedDataTestUtils.createDefaultHourlySegments(helper, tempFolder, closer))
                              .build();
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public String toString()
      {
        return "segments";
      }
    });
    return segmentsGenerators;
  }
}
