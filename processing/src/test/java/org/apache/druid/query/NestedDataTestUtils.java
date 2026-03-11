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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.ResourceInputSource;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

  public static final DimensionsSpec AUTO_DISCOVERY =
      DimensionsSpec.builder()
                    .useSchemaDiscovery(true)
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
      0,
      null
  );

  public static final List<String> SIMPLE_DATA_TSV_COLUMN_NAMES = Arrays.asList(
      "dim",
      "nest_json",
      "nester_json",
      "variant_json",
      "list_json",
      "nonexistent"
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
    JSON_MAPPER.registerModules(BuiltInTypesModule.getJacksonModulesList());
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
    return new ResourceFileSegmentBuilder(tempFolder, closer).inputSources(inputFiles)
                                                 .granularity(granularity)
                                                 .rollup(rollup)
                                                 .build();
  }

  public static class ResourceFileSegmentBuilder
  {
    private TemporaryFolder tempFolder;
    private Closer closer;

    private List<InputSource> inputSources =
        List.of(ResourceInputSource.of(NestedDataTestUtils.class.getClassLoader(), SIMPLE_DATA_FILE));
    private InputFormat inputFormat = TestIndex.DEFAULT_JSON_INPUT_FORMAT;
    private TimestampSpec timestampSpec = TIMESTAMP_SPEC;
    private DimensionsSpec dimensionsSpec = AUTO_DISCOVERY;
    private TransformSpec transformSpec = TransformSpec.NONE;
    private AggregatorFactory[] aggregators = COUNT;
    private Granularity queryGranularity = Granularities.NONE;
    private boolean rollup = true;
    private IndexSpec indexSpec = IndexSpec.getDefault();

    /**
     * Builder for an {@link IncrementalIndexSegment} or a list of{@link QueryableIndexSegment}, with some defaults:
     * <li>input is {@link #SIMPLE_DATA_FILE}</li>
     * <li>input format is {@link TestIndex#DEFAULT_JSON_INPUT_FORMAT}</li>
     * <li>use schema auto discovery</li>
     * <li>count aggregator</li>
     * <li>use NONE granularity</li>
     * <li>rollup is on by default</li>
     * <li>use the default index spec</li>
     */
    public ResourceFileSegmentBuilder(TemporaryFolder tempFolder, Closer closer)
    {
      this.tempFolder = tempFolder;
      this.closer = closer;
    }

    public ResourceFileSegmentBuilder input(String... inputs)
    {
      this.inputSources =
          Arrays.stream(inputs)
                .map(f -> ResourceInputSource.of(NestedDataTestUtils.class.getClassLoader(), f))
                .collect(Collectors.toList());
      return this;
    }

    public ResourceFileSegmentBuilder inputSources(List<InputSource> inputSources)
    {
      this.inputSources = inputSources;
      return this;
    }

    public ResourceFileSegmentBuilder inputFormat(InputFormat inputFormat)
    {
      this.inputFormat = inputFormat;
      return this;
    }

    public ResourceFileSegmentBuilder dimensionsSpec(DimensionsSpec dimensionsSpec)
    {
      this.dimensionsSpec = dimensionsSpec;
      return this;
    }

    public ResourceFileSegmentBuilder transformSpec(TransformSpec transformSpec)
    {
      this.transformSpec = transformSpec;
      return this;
    }

    public ResourceFileSegmentBuilder aggregators(AggregatorFactory[] aggregators)
    {
      this.aggregators = aggregators;
      return this;
    }

    public ResourceFileSegmentBuilder granularity(Granularity queryGranularity)
    {
      this.queryGranularity = queryGranularity;
      return this;
    }

    public ResourceFileSegmentBuilder rollup(boolean rollup)
    {
      this.rollup = rollup;
      return this;
    }

    public ResourceFileSegmentBuilder indexSpec(IndexSpec indexSpec)
    {
      this.indexSpec = indexSpec;
      return this;
    }

    public List<Segment> build() throws Exception
    {
      return createSegments(
          tempFolder,
          closer,
          inputSources,
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

    public Segment buildIncremental() throws Exception
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
                                     .inputSource(Iterables.getOnlyElement(inputSources))
                                     .inputFormat(inputFormat)
                                     .transform(transformSpec)
                                     .inputTmpDir(tempFolder.newFolder());

      return new IncrementalIndexSegment(bob.buildIncrementalIndex(), SegmentId.dummy("test_datasource"));
    }
  }

  /**
   * @deprecated Use {@link ResourceFileSegmentBuilder} instead.
   */
  @Deprecated
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
    segmentsGenerators.add(new BiFunction<>()
    {
      @Override
      public List<Segment> apply(TemporaryFolder tempFolder, Closer closer)
      {
        try {
          return ImmutableList.<Segment>builder()
                              .addAll(new ResourceFileSegmentBuilder(tempFolder, closer).input(jsonInputFile).build())
                              .add(new ResourceFileSegmentBuilder(tempFolder, null).input(jsonInputFile).buildIncremental())
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
    segmentsGenerators.add(new BiFunction<>()
    {
      @Override
      public List<Segment> apply(TemporaryFolder tempFolder, Closer closer)
      {
        try {
          return ImmutableList.of(
              new ResourceFileSegmentBuilder(tempFolder, null).input(jsonInputFile).buildIncremental(),
              new ResourceFileSegmentBuilder(tempFolder, null).input(jsonInputFile).buildIncremental()
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
    segmentsGenerators.add(new BiFunction<>()
    {
      @Override
      public List<Segment> apply(TemporaryFolder tempFolder, Closer closer)
      {
        try {
          return ImmutableList.<Segment>builder()
                              .addAll(new ResourceFileSegmentBuilder(tempFolder, closer).input(jsonInputFile).build())
                              .addAll(new ResourceFileSegmentBuilder(tempFolder, closer).input(jsonInputFile).build())
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
    segmentsGenerators.add(new BiFunction<>()
    {
      @Override
      public List<Segment> apply(TemporaryFolder tempFolder, Closer closer)
      {
        try {
          return Stream.of(
                           new StringEncodingStrategy.FrontCoded(4, (byte) 0x01),
                           new StringEncodingStrategy.FrontCoded(4, (byte) 0x00)
                       )
                       .map(strategy -> IndexSpec.builder().withStringDictionaryEncoding(strategy).build())
                       .map(indexSpec -> {
                         try {
                           return new ResourceFileSegmentBuilder(tempFolder, closer).input(jsonInputFile)
                                                                        .indexSpec(indexSpec)
                                                                        .build();
                         }
                         catch (Exception e) {
                           throw new RuntimeException(e);
                         }

                       }).flatMap(Collection::stream)
                       .collect(Collectors.toList());
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
