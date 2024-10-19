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

package org.apache.druid.benchmark.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchBuildAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.theta.SketchMergeAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SqlBenchmarkDatasets
{
  private static Map<String, BenchmarkSchema> DATASET_SCHEMAS = new HashMap<>();

  public static String BASIC = "basic";
  public static String EXPRESSIONS = "expressions";
  public static String NESTED = "nested";
  public static String DATASKETCHES = "datasketches";
  public static String PROJECTIONS = "projections";
  public static String GROUPER = "grouper";

  // initialize all benchmark dataset schemas to feed the data generators when running benchmarks, add any additional
  // datasets to this initializer as needed and they will be available to any benchmarks at the table name added here
  static {
    // the classic 'basic' schema, string dimension oriented with a few metrics
    final GeneratorSchemaInfo basicSchema = GeneratorBasicSchemas.SCHEMA_MAP.get(GeneratorBasicSchemas.BASIC_SCHEMA);
    DATASET_SCHEMAS.put(
        BASIC,
        new BenchmarkSchema(
            Collections.singletonList(makeSegment(BASIC, basicSchema.getDataInterval())),
            basicSchema,
            TransformSpec.NONE,
            makeDimensionsSpec(basicSchema),
            basicSchema.getAggsArray(),
            Collections.emptyList(),
            Granularities.NONE
        )
    );

    // expression testbench schema, lots of different column types
    final GeneratorSchemaInfo expressionsSchema = GeneratorBasicSchemas.SCHEMA_MAP.get(
        GeneratorBasicSchemas.EXPRESSION_TESTBENCH_SCHEMA
    );
    DATASET_SCHEMAS.put(
        EXPRESSIONS,
        new BenchmarkSchema(
            Collections.singletonList(makeSegment(EXPRESSIONS, expressionsSchema.getDataInterval())),
            expressionsSchema,
            TransformSpec.NONE,
            makeDimensionsSpec(expressionsSchema),
            expressionsSchema.getAggsArray(),
            Collections.emptyList(),
            Granularities.NONE
        )
    );

    // expressions schema but with transform to create nested column
    DATASET_SCHEMAS.put(
        NESTED,
        new BenchmarkSchema(
            Collections.singletonList(makeSegment(NESTED, expressionsSchema.getDataInterval())),
            expressionsSchema,
            new TransformSpec(
                null,
                ImmutableList.of(
                    new ExpressionTransform(
                        "nested",
                        "json_object('long1', long1, 'nesteder', json_object('string1', string1, 'long2', long2, 'double3',double3, 'string5', string5))",
                        TestExprMacroTable.INSTANCE
                    )
                )
            ),
            DimensionsSpec.builder().setDimensions(
                ImmutableList.copyOf(
                    Iterables.concat(
                        expressionsSchema.getDimensionsSpecExcludeAggs().getDimensions(),
                        Collections.singletonList(new AutoTypeColumnSchema("nested", null))
                    )
                )
            ).build(),
            expressionsSchema.getAggsArray(),
            Collections.emptyList(),
            Granularities.NONE
        )
    );

    // expressions schema but with some datasketch aggs defined
    GeneratorSchemaInfo datasketchesSchema = new GeneratorSchemaInfo(
            expressionsSchema.getColumnSchemas(),
            ImmutableList.of(
                new HllSketchBuildAggregatorFactory("hll_string5", "string5", null, null, null, false, true),
                new SketchMergeAggregatorFactory("theta_string5", "string5", null, null, null, null),
                new DoublesSketchAggregatorFactory("quantiles_float4", "float4", null, null, null),
                new DoublesSketchAggregatorFactory("quantiles_long3", "long3", null, null, null)
            ),
            expressionsSchema.getDataInterval(),
            true
    );
    DATASET_SCHEMAS.put(
        DATASKETCHES,
        new BenchmarkSchema(
            Collections.singletonList(makeSegment(DATASKETCHES, datasketchesSchema.getDataInterval())),
            datasketchesSchema,
            TransformSpec.NONE,
            makeDimensionsSpec(datasketchesSchema),
            datasketchesSchema.getAggsArray(),
            Collections.emptyList(),
            Granularities.NONE
        )
    );

    // expressions schema with projections
    DATASET_SCHEMAS.put(
        PROJECTIONS,
        new BenchmarkSchema(
            Collections.singletonList(makeSegment(PROJECTIONS, expressionsSchema.getDataInterval())),
            expressionsSchema,
            TransformSpec.NONE,
            makeDimensionsSpec(expressionsSchema),
            expressionsSchema.getAggsArray(),
            Arrays.asList(
                new AggregateProjectionSpec(
                    "string2_hourly_sums_hll",
                    VirtualColumns.create(
                        Granularities.toVirtualColumn(Granularities.HOUR, "__gran")
                    ),
                    Arrays.asList(
                        new StringDimensionSchema("string2"),
                        new LongDimensionSchema("__gran")
                    ),
                    new AggregatorFactory[]{
                        new LongSumAggregatorFactory("long4_sum", "long4"),
                        new DoubleSumAggregatorFactory("double2_sum", "double2"),
                        new HllSketchBuildAggregatorFactory("hll_string5", "string5", null, null, null, false, true)
                    }
                ),
                new AggregateProjectionSpec(
                    "string2_long2_sums",
                    VirtualColumns.EMPTY,
                    Arrays.asList(
                        new StringDimensionSchema("string2"),
                        new LongDimensionSchema("long2")
                    ),
                    new AggregatorFactory[]{
                        new LongSumAggregatorFactory("long4_sum", "long4"),
                        new DoubleSumAggregatorFactory("double2_sum", "double2"),
                        new HllSketchBuildAggregatorFactory("hll_string5", "string5", null, null, null, false, true)
                    }
                )
            ),
            Granularities.NONE
        )
    );

    // group-by testing, 2 segments
    final GeneratorSchemaInfo groupingSchema = GeneratorBasicSchemas.SCHEMA_MAP.get(
        GeneratorBasicSchemas.GROUPBY_TESTBENCH_SCHEMA
    );
    DATASET_SCHEMAS.put(
        GROUPER,
        new BenchmarkSchema(
            Arrays.asList(
                makeSegment(GROUPER, groupingSchema.getDataInterval(), 0),
                makeSegment(GROUPER, groupingSchema.getDataInterval(), 1)
            ),
            groupingSchema,
            new TransformSpec(
                null,
                ImmutableList.of(
                    // string array dims
                    new ExpressionTransform(
                        "stringArray-Sequential-100_000",
                        "array(\"string-Sequential-100_000\")",
                        TestExprMacroTable.INSTANCE
                    ),
                    new ExpressionTransform(
                        "stringArray-Sequential-3_000_000",
                        "array(\"string-Sequential-10_000_000\")",
                        TestExprMacroTable.INSTANCE
                    ),
                    /*
                    new ExpressionTransform(
                        "stringArray-Sequential-1_000_000_000",
                        "array(\"string-Sequential-1_000_000_000\")",
                        TestExprMacroTable.INSTANCE
                    ),*/
                    new ExpressionTransform(
                        "stringArray-ZipF-1_000_000",
                        "array(\"string-ZipF-1_000_000\")",
                        TestExprMacroTable.INSTANCE
                    ),
                    new ExpressionTransform(
                        "stringArray-Uniform-1_000_000",
                        "array(\"string-Uniform-1_000_000\")",
                        TestExprMacroTable.INSTANCE
                    ),

                    // long array dims
                    new ExpressionTransform(
                        "longArray-Sequential-100_000",
                        "array(\"long-Sequential-100_000\")",
                        TestExprMacroTable.INSTANCE
                    ),
                    new ExpressionTransform(
                        "longArray-Sequential-3_000_000",
                        "array(\"long-Sequential-10_000_000\")",
                        TestExprMacroTable.INSTANCE
                    ),
                    /*
                    new ExpressionTransform(
                        "longArray-Sequential-1_000_000_000",
                        "array(\"long-Sequential-1_000_000_000\")",
                        TestExprMacroTable.INSTANCE
                    ),*/
                    new ExpressionTransform(
                        "longArray-ZipF-1_000_000",
                        "array(\"long-ZipF-1_000_000\")",
                        TestExprMacroTable.INSTANCE
                    ),
                    new ExpressionTransform(
                        "longArray-Uniform-1_000_000",
                        "array(\"long-Uniform-1_000_000\")",
                        TestExprMacroTable.INSTANCE
                    ),

                    // nested complex json dim
                    new ExpressionTransform(
                        "nested-Sequential-100_000",
                        "json_object('long1', \"long-Sequential-100_000\", 'nesteder', json_object('long1', \"long-Sequential-100_000\"))",
                        TestExprMacroTable.INSTANCE
                    ),
                    new ExpressionTransform(
                        "nested-Sequential-3_000_000",
                        "json_object('long1', \"long-Sequential-10_000_000\", 'nesteder', json_object('long1', \"long-Sequential-10_000_000\"))",
                        TestExprMacroTable.INSTANCE
                    ),
                    /*
                    new ExpressionTransform(
                        "nested-Sequential-1_000_000_000",
                        "json_object('long1', \"long-Sequential-1_000_000_000\", 'nesteder', json_object('long1', \"long-Sequential-1_000_000_000\"))",
                        TestExprMacroTable.INSTANCE
                    ),*/
                    new ExpressionTransform(
                        "nested-ZipF-1_000_000",
                        "json_object('long1', \"long-ZipF-1_000_000\", 'nesteder', json_object('long1', \"long-ZipF-1_000_000\"))",
                        TestExprMacroTable.INSTANCE
                    ),
                    new ExpressionTransform(
                        "nested-Uniform-1_000_000",
                        "json_object('long1', \"long-Uniform-1_000_000\", 'nesteder', json_object('long1', \"long-Uniform-1_000_000\"))",
                        TestExprMacroTable.INSTANCE
                    )
                )
            ),
            makeDimensionsSpec(groupingSchema),
            groupingSchema.getAggsArray(),
            Collections.emptyList(),
            Granularities.NONE
        )
    );
  }

  public static BenchmarkSchema getSchema(String dataset)
  {
    return DATASET_SCHEMAS.get(dataset);
  }

  private static DataSegment makeSegment(String datasource, Interval interval)
  {
    return makeSegment(datasource, interval, 0);
  }

  private static DataSegment makeSegment(String datasource, Interval interval, int partitionNumber)
  {
    return DataSegment.builder()
                      .dataSource(datasource)
                      .interval(interval)
                      .version("1")
                      .shardSpec(new LinearShardSpec(partitionNumber))
                      .size(0)
                      .build();
  }

  private static DimensionsSpec makeDimensionsSpec(GeneratorSchemaInfo schemaInfo)
  {
    return DimensionsSpec.builder().setDimensions(schemaInfo.getDimensionsSpecExcludeAggs().getDimensions()).build();
  }

  public static class BenchmarkSchema
  {
    private final List<DataSegment> dataSegments;
    private final GeneratorSchemaInfo generatorSchemaInfo;
    private final TransformSpec transformSpec;
    private final DimensionsSpec dimensionsSpec;
    private final AggregatorFactory[] aggregators;
    private final Granularity queryGranularity;
    private final List<AggregateProjectionSpec> projections;

    public BenchmarkSchema(
        List<DataSegment> dataSegments,
        GeneratorSchemaInfo generatorSchemaInfo,
        TransformSpec transformSpec,
        DimensionsSpec dimensionSpec,
        AggregatorFactory[] aggregators,
        List<AggregateProjectionSpec> projections,
        Granularity queryGranularity
    )
    {
      this.dataSegments = dataSegments;
      this.generatorSchemaInfo = generatorSchemaInfo;
      this.transformSpec = transformSpec;
      this.dimensionsSpec = dimensionSpec;
      this.aggregators = aggregators;
      this.queryGranularity = queryGranularity;
      this.projections = projections;
    }

    public List<DataSegment> getDataSegments()
    {
      return dataSegments;
    }

    public GeneratorSchemaInfo getGeneratorSchemaInfo()
    {
      return generatorSchemaInfo;
    }

    public TransformSpec getTransformSpec()
    {
      return transformSpec;
    }

    public DimensionsSpec getDimensionsSpec()
    {
      return dimensionsSpec;
    }

    public AggregatorFactory[] getAggregators()
    {
      return aggregators;
    }

    public Granularity getQueryGranularity()
    {
      return queryGranularity;
    }

    public List<AggregateProjectionSpec> getProjections()
    {
      return projections;
    }

    public BenchmarkSchema asAutoDimensions()
    {
      return new SqlBenchmarkDatasets.BenchmarkSchema(
          dataSegments,
          generatorSchemaInfo,
          transformSpec,
          dimensionsSpec.withDimensions(
                    dimensionsSpec.getDimensions()
                                  .stream()
                                  .map(dim -> new AutoTypeColumnSchema(dim.getName(), null))
                                  .collect(Collectors.toList())
          ),
          aggregators,
          projections.stream()
                     .map(projection -> new AggregateProjectionSpec(
                         projection.getName(),
                         projection.getVirtualColumns(),
                         projection.getGroupingColumns()
                                   .stream()
                                   .map(dim -> new AutoTypeColumnSchema(dim.getName(), null))
                                   .collect(Collectors.toList()),
                         projection.getAggregators()
                     )).collect(Collectors.toList()),
          queryGranularity
      );
    }
  }
}
