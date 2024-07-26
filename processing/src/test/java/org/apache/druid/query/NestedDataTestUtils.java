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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.ResourceInputSource;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.NestedDataColumnSchema;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.timeline.SegmentId;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

public class NestedDataTestUtils
{
  public static final String SIMPLE_DATA_FILE = "nested-simple-test-data.json";
  public static final String SIMPLE_DATA_TSV_FILE = "nested-simple-test-data.tsv";
  public static final String NUMERIC_DATA_FILE = "nested-numeric-test-data.json";
  public static final String TYPES_DATA_FILE = "nested-types-test-data.json";
  public static final String ARRAY_TYPES_DATA_FILE = "nested-array-test-data.json";

  public static final String ARRAY_TYPES_DATA_FILE_2 = "nested-array-test-data-2.json";
  public static final String ALL_TYPES_TEST_DATA_FILE = "nested-all-types-test-data.json";

  public static final String INCREMENTAL_SEGMENTS_NAME = "incremental";
  public static final String DEFAULT_SEGMENTS_NAME = "segments";
  public static final String FRONT_CODED_SEGMENTS_NAME = "segments-frontcoded";
  public static final String MIX_SEGMENTS_NAME = "mixed";

  public static final ObjectMapper JSON_MAPPER;

  public static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec("timestamp", null, null);

  public static final JsonInputFormat DEFAULT_JSON_INPUT_FORMAT = new JsonInputFormat(
      JSONPathSpec.DEFAULT,
      null,
      null,
      null,
      null
  );

  public static final DimensionsSpec AUTO_DISCOVERY =
      DimensionsSpec.builder()
                    .useSchemaDiscovery(true)
                    .build();

  public static final DimensionsSpec TSV_SCHEMA =
      DimensionsSpec.builder()
                    .setDimensions(
                        Arrays.asList(
                            new AutoTypeColumnSchema("dim", null),
                            new AutoTypeColumnSchema("nest_json", null),
                            new AutoTypeColumnSchema("nester_json", null),
                            new AutoTypeColumnSchema("variant_json", null),
                            new AutoTypeColumnSchema("list_json", null),
                            new AutoTypeColumnSchema("nonexistent", null)
                        )
                    )
                    .build();

  public static final DimensionsSpec TSV_V4_SCHEMA =
      DimensionsSpec.builder()
                    .setDimensions(
                        Arrays.asList(
                            new NestedDataColumnSchema("dim", 4),
                            new NestedDataColumnSchema("nest_json", 4),
                            new NestedDataColumnSchema("nester_json", 4),
                            new NestedDataColumnSchema("variant_json", 4),
                            new NestedDataColumnSchema("list_json", 4),
                            new NestedDataColumnSchema("nonexistent", 4)
                        )
                    )
                    .build();
  public static final InputRowSchema AUTO_SCHEMA = new InputRowSchema(
      TIMESTAMP_SPEC,
      AUTO_DISCOVERY,
      null
  );

  public static DelimitedInputFormat SIMPLE_DATA_TSV_INPUT_FORMAT = new DelimitedInputFormat(
      Arrays.asList(
          "timestamp",
          "dim",
          "nest",
          "nester",
          "variant",
          "list"
      ),
      null,
      null,
      false,
      false,
      0
  );

  public static final TransformSpec SIMPLE_DATA_TSV_TRANSFORM = new TransformSpec(
      null,
      Arrays.asList(
          new ExpressionTransform("nest_json", "parse_json(nest)", TestExprMacroTable.INSTANCE),
          new ExpressionTransform("nester_json", "parse_json(nester)", TestExprMacroTable.INSTANCE),
          new ExpressionTransform("variant_json", "parse_json(variant)", TestExprMacroTable.INSTANCE),
          new ExpressionTransform("list_json", "parse_json(list)", TestExprMacroTable.INSTANCE)
      )
  );

  public static final AggregatorFactory[] COUNT = new AggregatorFactory[]{
      new CountAggregatorFactory("count")
  };

  static {
    JSON_MAPPER = TestHelper.makeJsonMapper();
    JSON_MAPPER.registerModules(NestedDataModule.getJacksonModulesList());
  }

  public static List<Segment> createSimpleSegmentsTsv(
      TemporaryFolder tempFolder,
      Closer closer
  )
      throws Exception
  {
    return createSimpleNestedTestDataTsvSegments(
        tempFolder,
        closer,
        Granularities.NONE,
        TSV_SCHEMA,
        true
    );
  }

  public static List<Segment> createSimpleSegmentsTsvV4(
      TemporaryFolder tempFolder,
      Closer closer
  )
      throws Exception
  {
    return createSimpleNestedTestDataTsvSegments(
        tempFolder,
        closer,
        Granularities.NONE,
        TSV_V4_SCHEMA,
        true
    );
  }

  public static List<Segment> createSimpleNestedTestDataTsvSegments(
      TemporaryFolder tempFolder,
      Closer closer,
      Granularity granularity,
      DimensionsSpec dimensionsSpec,
      boolean rollup
  ) throws Exception
  {
    return createSegments(
        tempFolder,
        closer,
        SIMPLE_DATA_TSV_FILE,
        SIMPLE_DATA_TSV_INPUT_FORMAT,
        TIMESTAMP_SPEC,
        dimensionsSpec,
        SIMPLE_DATA_TSV_TRANSFORM,
        COUNT,
        granularity,
        rollup,
        IndexSpec.DEFAULT
    );
  }

  public static Segment createSimpleNestedTestDataIncrementalIndex(TemporaryFolder tempFolder) throws Exception
  {
    return createIncrementalIndexForJsonInput(
        tempFolder,
        SIMPLE_DATA_FILE,
        Granularities.NONE,
        true
    );
  }

  public static List<Segment> createSimpleNestedTestDataSegments(
      TemporaryFolder tempFolder,
      Closer closer
  )
      throws Exception
  {
    return createSegmentsForJsonInput(
        tempFolder,
        closer,
        SIMPLE_DATA_FILE,
        Granularities.NONE,
        true,
        IndexSpec.DEFAULT
    );
  }

  public static Segment createIncrementalIndexForJsonInput(TemporaryFolder tempFolder, String fileName)
      throws Exception
  {
    return createIncrementalIndexForJsonInput(
        tempFolder,
        fileName,
        Granularities.NONE,
        true
    );
  }

  public static Segment createIncrementalIndexForJsonInput(
      TemporaryFolder tempFolder,
      String file,
      Granularity granularity,
      boolean rollup
  )
      throws Exception
  {
    return createIncrementalIndex(
        tempFolder,
        file,
        DEFAULT_JSON_INPUT_FORMAT,
        TIMESTAMP_SPEC,
        AUTO_DISCOVERY,
        TransformSpec.NONE,
        COUNT,
        granularity,
        rollup
    );
  }

  public static List<Segment> createSegmentsForJsonInput(
      TemporaryFolder tempFolder,
      Closer closer,
      String inputFile,
      Granularity granularity,
      boolean rollup,
      IndexSpec indexSpec
  ) throws Exception
  {
    return createSegments(
        tempFolder,
        closer,
        inputFile,
        DEFAULT_JSON_INPUT_FORMAT,
        TIMESTAMP_SPEC,
        AUTO_DISCOVERY,
        TransformSpec.NONE,
        COUNT,
        granularity,
        rollup,
        indexSpec
    );
  }

  public static List<Segment> createSegmentsWithConcatenatedJsonInput(
      TemporaryFolder tempFolder,
      Closer closer,
      String inputFile,
      Granularity granularity,
      boolean rollup,
      int numCopies,
      int numSegments
  ) throws Exception
  {
    List<InputSource> inputFiles = Lists.newArrayListWithCapacity(numSegments);
    for (int i = 0; i < numSegments; i++) {
      File file = selfConcatenateResourceFile(tempFolder, inputFile, numCopies);
      inputFiles.add(new LocalInputSource(file.getParentFile(), file.getName()));
    }
    return createSegments(
        tempFolder,
        closer,
        inputFiles,
        DEFAULT_JSON_INPUT_FORMAT,
        TIMESTAMP_SPEC,
        AUTO_DISCOVERY,
        TransformSpec.NONE,
        COUNT,
        granularity,
        rollup,
        IndexSpec.DEFAULT
    );
  }

  public static List<Segment> createSegmentsForJsonInput(
      TemporaryFolder tempFolder,
      Closer closer,
      String inputFile,
      IndexSpec indexSpec
  )
      throws Exception
  {
    return createSegmentsForJsonInput(
        tempFolder,
        closer,
        inputFile,
        Granularities.NONE,
        true,
        indexSpec
    );
  }

  public static Segment createIncrementalIndex(
      TemporaryFolder tempFolder,
      String inputFileName,
      InputFormat inputFormat,
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      TransformSpec transformSpec,
      AggregatorFactory[] aggregators,
      Granularity queryGranularity,
      boolean rollup
  )
      throws Exception
  {
    IndexBuilder bob = IndexBuilder.create()
                                   .schema(
                                       IncrementalIndexSchema.builder()
                                                             .withTimestampSpec(timestampSpec)
                                                             .withDimensionsSpec(dimensionsSpec)
                                                             .withMetrics(aggregators)
                                                             .withQueryGranularity(queryGranularity)
                                                             .withRollup(rollup)
                                                             .withMinTimestamp(0)
                                                             .build()
                                   )
                                   .inputSource(
                                       ResourceInputSource.of(
                                           NestedDataTestUtils.class.getClassLoader(),
                                           inputFileName
                                       )
                                   )
                                   .inputFormat(inputFormat)
                                   .transform(transformSpec)
                                   .inputTmpDir(tempFolder.newFolder());

    return new IncrementalIndexSegment(bob.buildIncrementalIndex(), SegmentId.dummy("test_datasource"));
  }

  public static List<Segment> createSegments(
      TemporaryFolder tempFolder,
      Closer closer,
      String input,
      InputFormat inputFormat,
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      TransformSpec transformSpec,
      AggregatorFactory[] aggregators,
      Granularity queryGranularity,
      boolean rollup,
      IndexSpec indexSpec
  ) throws Exception
  {
    return createSegments(
        tempFolder,
        closer,
        Collections.singletonList(ResourceInputSource.of(NestedDataTestUtils.class.getClassLoader(), input)),
        inputFormat,
        timestampSpec,
        dimensionsSpec,
        transformSpec,
        aggregators,
        queryGranularity,
        rollup,
        indexSpec
    );
  }

  public static List<Segment> createSegments(
      TemporaryFolder tempFolder,
      Closer closer,
      List<InputSource> inputs,
      InputFormat inputFormat,
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      TransformSpec transformSpec,
      AggregatorFactory[] aggregators,
      Granularity queryGranularity,
      boolean rollup,
      IndexSpec indexSpec
  ) throws Exception
  {
    final List<Segment> segments = Lists.newArrayListWithCapacity(inputs.size());
    for (InputSource inputSource : inputs) {
      final File segmentDir = tempFolder.newFolder();
      IndexBuilder bob = IndexBuilder.create()
                                     .tmpDir(segmentDir)
                                     .schema(
                                         IncrementalIndexSchema.builder()
                                                               .withTimestampSpec(timestampSpec)
                                                               .withDimensionsSpec(dimensionsSpec)
                                                               .withMetrics(aggregators)
                                                               .withQueryGranularity(queryGranularity)
                                                               .withRollup(rollup)
                                                               .withMinTimestamp(0)
                                                               .build()
                                     )
                                     .indexSpec(indexSpec)
                                     .inputSource(inputSource)
                                     .inputFormat(inputFormat)
                                     .transform(transformSpec)
                                     .inputTmpDir(tempFolder.newFolder());
      segments.add(
          new QueryableIndexSegment(
              closer.register(bob.buildMMappedIndex()),
              SegmentId.dummy("test_datasource")
          )
      );
    }

    return segments;
  }

  /**
   * turn a small file into bigger file with a bunch of copies of itself
   */
  public static File selfConcatenateResourceFile(
      TemporaryFolder tempFolder,
      String inputFileName,
      int numCopies
  ) throws IOException
  {
    List<InputStream> inputStreams = Lists.newArrayListWithCapacity(numCopies);
    for (int i = 0; i < numCopies; i++) {
      InputStream stream = NestedDataTestUtils.class.getClassLoader().getResourceAsStream(inputFileName);
      inputStreams.add(stream);
      if (i + 1 < numCopies) {
        inputStreams.add(new ByteArrayInputStream(StringUtils.toUtf8("\n")));
      }
    }
    File tmpFile = tempFolder.newFile();
    try (
        SequenceInputStream inputDataStream = new SequenceInputStream(Collections.enumeration(inputStreams));
        OutputStream outStream = Files.newOutputStream(tmpFile.toPath())
    ) {
      final byte[] buffer = new byte[8096];
      int bytesRead;
      while ((bytesRead = inputDataStream.read(buffer)) != -1) {
        outStream.write(buffer, 0, bytesRead);
      }
    }

    return tmpFile;
  }

  public static List<BiFunction<TemporaryFolder, Closer, List<Segment>>> getSegmentGenerators(
      String jsonInputFile
  )
  {
    final List<BiFunction<TemporaryFolder, Closer, List<Segment>>> segmentsGenerators =
        new ArrayList<>();
    segmentsGenerators.add(new BiFunction<TemporaryFolder, Closer, List<Segment>>()
    {
      @Override
      public List<Segment> apply(TemporaryFolder tempFolder, Closer closer)
      {
        try {
          return ImmutableList.<Segment>builder()
                              .addAll(
                                  NestedDataTestUtils.createSegmentsForJsonInput(
                                      tempFolder,
                                      closer,
                                      jsonInputFile,
                                      IndexSpec.DEFAULT
                                  )
                              )
                              .add(NestedDataTestUtils.createIncrementalIndexForJsonInput(tempFolder, jsonInputFile))
                              .build();
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public String toString()
      {
        return MIX_SEGMENTS_NAME;
      }
    });
    segmentsGenerators.add(new BiFunction<TemporaryFolder, Closer, List<Segment>>()
    {
      @Override
      public List<Segment> apply(TemporaryFolder tempFolder, Closer closer)
      {
        try {
          return ImmutableList.of(
              NestedDataTestUtils.createIncrementalIndexForJsonInput(tempFolder, jsonInputFile),
              NestedDataTestUtils.createIncrementalIndexForJsonInput(tempFolder, jsonInputFile)
          );
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public String toString()
      {
        return INCREMENTAL_SEGMENTS_NAME;
      }
    });
    segmentsGenerators.add(new BiFunction<TemporaryFolder, Closer, List<Segment>>()
    {
      @Override
      public List<Segment> apply(TemporaryFolder tempFolder, Closer closer)
      {
        try {
          return ImmutableList.<Segment>builder()
                              .addAll(
                                  NestedDataTestUtils.createSegmentsForJsonInput(
                                      tempFolder,
                                      closer,
                                      jsonInputFile,
                                      IndexSpec.DEFAULT
                                  )
                              )
                              .addAll(
                                  NestedDataTestUtils.createSegmentsForJsonInput(
                                      tempFolder,
                                      closer,
                                      jsonInputFile,
                                      IndexSpec.DEFAULT
                                  )
                              )
                              .build();
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public String toString()
      {
        return DEFAULT_SEGMENTS_NAME;
      }
    });
    segmentsGenerators.add(new BiFunction<TemporaryFolder, Closer, List<Segment>>()
    {
      @Override
      public List<Segment> apply(TemporaryFolder tempFolder, Closer closer)
      {
        try {
          return ImmutableList.<Segment>builder()
                              .addAll(
                                  NestedDataTestUtils.createSegmentsForJsonInput(
                                      tempFolder,
                                      closer,
                                      jsonInputFile,
                                      IndexSpec.builder()
                                               .withStringDictionaryEncoding(
                                                   new StringEncodingStrategy.FrontCoded(4, (byte) 0x01)
                                               )
                                               .build()
                                  )
                              )
                              .addAll(
                                  NestedDataTestUtils.createSegmentsForJsonInput(
                                      tempFolder,
                                      closer,
                                      jsonInputFile,
                                      IndexSpec.builder()
                                               .withStringDictionaryEncoding(
                                                   new StringEncodingStrategy.FrontCoded(4, (byte) 0x00)
                                               )
                                               .build()
                                  )
                              )
                              .build();
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public String toString()
      {
        return FRONT_CODED_SEGMENTS_NAME;
      }
    });
    return segmentsGenerators;
  }

  public static boolean expectSegmentGeneratorCanVectorize(String name)
  {
    return DEFAULT_SEGMENTS_NAME.equals(name) || FRONT_CODED_SEGMENTS_NAME.equals(name);
  }
}
