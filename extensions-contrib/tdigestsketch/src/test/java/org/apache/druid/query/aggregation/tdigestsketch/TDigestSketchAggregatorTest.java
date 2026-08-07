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

package org.apache.druid.query.aggregation.tdigestsketch;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class TDigestSketchAggregatorTest extends InitializedNullHandlingTest
{
  private AggregationTestHelper helper;

  @TempDir
  public File tempFolder;

  public void initTDigestSketchAggregatorTest(final GroupByQueryConfig config)
  {
    TDigestSketchModule.registerSerde();
    TDigestSketchModule module = new TDigestSketchModule();
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelperWithTempDir(
        module.getJacksonModules(), config, tempFolder);
    InjectableValues currentInjectableValues = helper.getObjectMapper().getInjectableValues();
    InjectableValues.Std currentInjectableValuesStd = (InjectableValues.Std) currentInjectableValues;
    currentInjectableValuesStd.addValue(TDigestConfig.class.getName(), TDigestConfig.builder().build());
    helper.getObjectMapper().setInjectableValues(currentInjectableValuesStd);
  }

  private static List<GroupByQueryConfig> testConfigs()
  {
    return List.of(
        new GroupByQueryConfig()
        {
          @Override
          public int getBufferGrouperInitialBuckets()
          {
            return 4;
          }
        },
        new GroupByQueryConfig()
        {
          @Override
          public int getBufferGrouperMaxSize()
          {
            return 2;
          }

          @Override
          public HumanReadableBytes getMaxOnDiskStorage()
          {
            return HumanReadableBytes.valueOf(10L * 1024 * 1024);
          }
        },
        new org.apache.druid.jackson.DefaultObjectMapper().convertValue(
            java.util.Map.of(
                "maxSelectorDictionarySize", 20,
                "maxMergingDictionarySize", 400,
                "maxOnDiskStorage", 10L * 1024 * 1024
            ),
            GroupByQueryConfig.class
        ),
        new GroupByQueryConfig()
        {
          @Override
          public int getNumParallelCombineThreads()
          {
            return 2;
          }
        }
    );
  }

  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : testConfigs()) {
      constructors.add(new Object[]{config});
    }
    return constructors;
  }

  // this is to test Json properties and equals
  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void serializeDeserializeFactoryWithFieldName(final GroupByQueryConfig config) throws Exception
  {
    initTDigestSketchAggregatorTest(config);
    ObjectMapper objectMapper = new DefaultObjectMapper();
    objectMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(TDigestConfig.class.getName(), TDigestConfig.builder().build())
    );
    new TDigestSketchModule().getJacksonModules().forEach(objectMapper::registerModule);
    TDigestSketchAggregatorFactory factory = new TDigestSketchAggregatorFactory("name", "filedName", 128, TDigestConfig.builder().build());

    AggregatorFactory other = objectMapper.readValue(
        objectMapper.writeValueAsString(factory),
        AggregatorFactory.class
    );

    Assertions.assertEquals(factory, other);
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void deserializedFactoryCompressionCappedAtMaxCompression(final GroupByQueryConfig config) throws Exception
  {
    initTDigestSketchAggregatorTest(config);
    ObjectMapper objectMapper = new DefaultObjectMapper();
    objectMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(TDigestConfig.class.getName(), TDigestConfig.builder().maxCompression(150).build())
    );
    new TDigestSketchModule().getJacksonModules().forEach(objectMapper::registerModule);
    TDigestSketchAggregatorFactory factory = new TDigestSketchAggregatorFactory("name", "fieldName", 300, TDigestConfig.builder().maxCompression(150).build());

    TDigestSketchAggregatorFactory deserialized = (TDigestSketchAggregatorFactory) objectMapper.readValue(
        objectMapper.writeValueAsString(factory),
        AggregatorFactory.class
    );

    Assertions.assertEquals(150, deserialized.getCompression());
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void deserializedFactoryCompressionBelowMaxCompressionUnchanged(final GroupByQueryConfig config) throws Exception
  {
    initTDigestSketchAggregatorTest(config);
    ObjectMapper objectMapper = new DefaultObjectMapper();
    objectMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(TDigestConfig.class.getName(), TDigestConfig.builder().maxCompression(150).build())
    );
    new TDigestSketchModule().getJacksonModules().forEach(objectMapper::registerModule);
    TDigestSketchAggregatorFactory factory = new TDigestSketchAggregatorFactory("name", "fieldName", 100, TDigestConfig.builder().maxCompression(150).build());

    TDigestSketchAggregatorFactory deserialized = (TDigestSketchAggregatorFactory) objectMapper.readValue(
        objectMapper.writeValueAsString(factory),
        AggregatorFactory.class
    );

    Assertions.assertEquals(100, deserialized.getCompression());
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void buildingSketchesAtIngestionTime(final GroupByQueryConfig config) throws Exception
  {
    initTDigestSketchAggregatorTest(config);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("doubles_build_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            DimensionsSpec.builder()
                          .setDefaultSchemaDimensions(List.of("product"))
                          .setDimensionExclusions(List.of("sequenceNumber"))
                          .build(),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "sequenceNumber", "product", "value")
        ),
        List.of(new TDigestSketchAggregatorFactory("sketch", "value", 200, TDigestConfig.builder().build())),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setDimensions(Collections.emptyList())
                    .setAggregatorSpecs(new TDigestSketchAggregatorFactory("merged_sketch", "sketch", 200, TDigestConfig.builder().build()))
                    .setPostAggregatorSpecs(
                        new TDigestSketchToQuantilesPostAggregator(
                            "quantiles",
                            new FieldAccessPostAggregator("merged_sketch", "merged_sketch"),
                            new double[]{0, 0.5, 1}
                        )
                    )
                    .setInterval(Intervals.of("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z"))
                    .build()
    );
    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    ResultRow row = results.get(0);

    // post agg
    Object quantilesObject = row.get(1); // "quantiles"
    Assertions.assertTrue(quantilesObject instanceof double[]);
    double[] quantiles = (double[]) quantilesObject;
    Assertions.assertEquals(0.001, quantiles[0], 0.0006); // min value
    Assertions.assertEquals(0.5, quantiles[1], 0.05); // median value
    Assertions.assertEquals(1, quantiles[2], 0.05); // max value
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void buildingSketchesAtQueryTime(final GroupByQueryConfig config) throws Exception
  {
    initTDigestSketchAggregatorTest(config);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("doubles_build_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("sequenceNumber", "product"))),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "sequenceNumber", "product", "value")
        ),
        List.of(new DoubleSumAggregatorFactory("value", "value")),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setDimensions(Collections.emptyList())
                    .setAggregatorSpecs(new TDigestSketchAggregatorFactory("sketch", "value", 200, TDigestConfig.builder().build()))
                    .setPostAggregatorSpecs(
                        new TDigestSketchToQuantilesPostAggregator(
                            "quantiles",
                            new FieldAccessPostAggregator("sketch", "sketch"),
                            new double[]{0, 0.5, 1}
                        )
                    )
                    .setInterval(Intervals.of("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z"))
                    .build()
    );
    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    ResultRow row = results.get(0);


    // post agg
    Object quantilesObject = row.get(1); // "quantiles"
    Assertions.assertTrue(quantilesObject instanceof double[]);
    double[] quantiles = (double[]) quantilesObject;
    Assertions.assertEquals(0.001, quantiles[0], 0.0006); // min value
    Assertions.assertEquals(0.5, quantiles[1], 0.05); // median value
    Assertions.assertEquals(1, quantiles[2], 0.05); // max value
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testIngestingSketches(final GroupByQueryConfig config) throws Exception
  {
    initTDigestSketchAggregatorTest(config);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("doubles_sketch_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("product"))),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "product", "sketch")
        ),
        List.of(new TDigestSketchAggregatorFactory("first_level_merge_sketch", "sketch", 200, TDigestConfig.builder().build())),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setDimensions(Collections.emptyList())
                    .setAggregatorSpecs(
                        new TDigestSketchAggregatorFactory("second_level_merge_sketch", "first_level_merge_sketch", 200, TDigestConfig.builder().build())
                    )
                    .setPostAggregatorSpecs(
                        new TDigestSketchToQuantilesPostAggregator(
                            "quantiles",
                            new FieldAccessPostAggregator("second_level_merge_sketch", "second_level_merge_sketch"),
                            new double[]{0, 0.5, 1}
                        )
                    )
                    .setInterval(Intervals.of("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z"))
                    .build()
    );
    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    ResultRow row = results.get(0);

    // post agg
    Object quantilesObject = row.get(1); // "quantiles"
    Assertions.assertTrue(quantilesObject instanceof double[]);
    double[] quantiles = (double[]) quantilesObject;
    Assertions.assertEquals(0.001, quantiles[0], 0.0006); // min value
    Assertions.assertEquals(0.5, quantiles[1], 0.05); // median value
    Assertions.assertEquals(1, quantiles[2], 0.05); // max value
  }
}
