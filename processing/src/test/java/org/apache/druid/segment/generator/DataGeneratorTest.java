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

package org.apache.druid.segment.generator;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

// Doesn't assert behavior right now, just generates rows and prints out some distribution numbers
public class DataGeneratorTest extends InitializedNullHandlingTest
{
  private static final Logger log = new Logger(DataGeneratorTest.class);

  @Test
  public void testSequential()
  {
    List<GeneratorColumnSchema> schemas = new ArrayList<>();
    RowValueTracker tracker = new RowValueTracker();

    schemas.add(
        GeneratorColumnSchema.makeSequential(
            "dimA",
            ValueType.STRING,
            false,
            1,
            null,
            10,
            20
        )
    );

    schemas.add(
        GeneratorColumnSchema.makeEnumeratedSequential(
            "dimB",
            ValueType.STRING,
            false,
            1,
            null,
            Arrays.asList("Hello", "World", "Foo", "Bar")
        )
    );

    schemas.add(
        GeneratorColumnSchema.makeSequential(
            "dimC",
            ValueType.STRING,
            false,
            1,
            0.50,
            30,
            40
        )
    );

    DataGenerator dataGenerator = new DataGenerator(schemas, 9999, 0, 0, 1000.0);
    for (int i = 0; i < 100; i++) {
      InputRow row = dataGenerator.nextRow();
      tracker.addRow(row);
    }
    tracker.printStuff();
    tracker.assertTotalRowSize("dimA", 100);
    tracker.assertTotalRowSize("dimB", 100);
    tracker.assertTotalRowSize("dimC", 100);
  }

  @Test
  public void testDiscreteUniform()
  {
    List<GeneratorColumnSchema> schemas = new ArrayList<>();
    RowValueTracker tracker = new RowValueTracker();

    schemas.add(
        GeneratorColumnSchema.makeDiscreteUniform(
            "dimA",
            ValueType.STRING,
            false,
            1,
            null,
            10,
            20
        )
    );

    schemas.add(
        GeneratorColumnSchema.makeEnumeratedDiscreteUniform(
            "dimB",
            ValueType.STRING,
            false,
            4,
            null,
            Arrays.asList("Hello", "World", "Foo", "Bar")
        )
    );

    schemas.add(
        GeneratorColumnSchema.makeDiscreteUniform(
            "dimC",
            ValueType.STRING,
            false,
            1,
            0.50,
            10,
            20
        )
    );

    schemas.add(
        GeneratorColumnSchema.makeDiscreteUniform(
            "dimD",
            ValueType.FLOAT,
            false,
            1,
            null,
            100,
            120
        )
    );

    DataGenerator dataGenerator = new DataGenerator(schemas, 9999, 0, 0, 1000.0);
    for (int i = 0; i < 100; i++) {
      InputRow row = dataGenerator.nextRow();
      tracker.addRow(row);
    }

    tracker.printStuff();
    tracker.assertTotalRowSize("dimA", 100);
    // The row size of dimB is 4, dataGenerator will generate 4 value for dimB at one time, so the total size is 400
    tracker.assertTotalRowSize("dimB", 400);
    tracker.assertTotalRowSize("dimC", 100);
    tracker.assertNullable("dimC", true);
    tracker.assertTotalRowSize("dimD", 100);
  }


  @Test
  public void testRoundedNormal()
  {
    List<GeneratorColumnSchema> schemas = new ArrayList<>();
    RowValueTracker tracker = new RowValueTracker();

    schemas.add(
        GeneratorColumnSchema.makeNormal(
            "dimA",
            ValueType.FLOAT,
            false,
            1,
            null,
            50.0,
            1.0,
            true
        )
    );

    schemas.add(
        GeneratorColumnSchema.makeNormal(
            "dimB",
            ValueType.STRING,
            false,
            1,
            null,
            1000.0,
            10.0,
            true
        )
    );

    DataGenerator dataGenerator = new DataGenerator(schemas, 9999, 0, 0, 1000.0);
    for (int i = 0; i < 1000000; i++) {
      InputRow row = dataGenerator.nextRow();
      tracker.addRow(row);
    }

    tracker.printStuff();
    tracker.assertTotalRowSize("dimA", 1000000);
    tracker.assertTotalRowSize("dimB", 1000000);
  }

  @Test
  public void testZipf()
  {
    List<GeneratorColumnSchema> schemas = new ArrayList<>();
    RowValueTracker tracker = new RowValueTracker();

    schemas.add(
        GeneratorColumnSchema.makeZipf(
            "dimA",
            ValueType.STRING,
            false,
            1,
            null,
            1000,
            2000,
            1.0
        )
    );

    schemas.add(
        GeneratorColumnSchema.makeZipf(
            "dimB",
            ValueType.FLOAT,
            false,
            1,
            null,
            99990,
            99999,
            1.0
        )
    );

    schemas.add(
        GeneratorColumnSchema.makeEnumeratedZipf(
            "dimC",
            ValueType.STRING,
            false,
            1,
            null,
            Arrays.asList("1-Hello", "2-World", "3-Foo", "4-Bar", "5-BA5EBA11", "6-Rocky", "7-Mango", "8-Contango"),
            1.0
        )
    );

    DataGenerator dataGenerator = new DataGenerator(schemas, 9999, 0, 0, 1000.0);
    for (int i = 0; i < 100; i++) {
      InputRow row = dataGenerator.nextRow();
      tracker.addRow(row);
    }

    tracker.printStuff();
    tracker.assertTotalRowSize("dimA", 100);
    tracker.assertTotalRowSize("dimB", 100);
    tracker.assertTotalRowSize("dimC", 100);
  }

  @Test
  public void testEnumerated()
  {
    List<GeneratorColumnSchema> schemas = new ArrayList<>();
    RowValueTracker tracker = new RowValueTracker();

    schemas.add(
        GeneratorColumnSchema.makeEnumerated(
            "dimA",
            ValueType.STRING,
            false,
            1,
            null,
            Arrays.asList("Hello", "World", "Foo", "Bar"),
            Arrays.asList(0.5, 0.25, 0.15, 0.10)
        )
    );

    DataGenerator dataGenerator = new DataGenerator(schemas, 9999, 0, 0, 1000.0);
    for (int i = 0; i < 10000; i++) {
      InputRow row = dataGenerator.nextRow();
      tracker.addRow(row);
    }

    tracker.printStuff();
    tracker.assertTotalRowSize("dimA", 10000);
  }

  @Test
  public void testNormal()
  {
    List<GeneratorColumnSchema> schemas = new ArrayList<>();
    RowValueTracker tracker = new RowValueTracker();

    schemas.add(
        GeneratorColumnSchema.makeNormal(
            "dimA",
            ValueType.FLOAT,
            false,
            1,
            null,
            8.0,
            1.0,
            false
        )
    );

    schemas.add(
        GeneratorColumnSchema.makeNormal(
            "dimB",
            ValueType.STRING,
            false,
            1,
            0.50,
            88.0,
            2.0,
            false
        )
    );

    DataGenerator dataGenerator = new DataGenerator(schemas, 9999, 0, 0, 1000.0);
    for (int i = 0; i < 100; i++) {
      InputRow row = dataGenerator.nextRow();
      tracker.addRow(row);
    }

    tracker.printStuff();
    tracker.assertTotalRowSize("dimA", 100);
    tracker.assertTotalRowSize("dimB", 100);
    tracker.assertNullable("dimB", true);
  }

  @Test
  public void testRealUniform()
  {
    List<GeneratorColumnSchema> schemas = new ArrayList<>();
    RowValueTracker tracker = new RowValueTracker();

    schemas.add(
        GeneratorColumnSchema.makeContinuousUniform(
            "dimA",
            ValueType.STRING,
            false,
            1,
            null,
            10.0,
            50.0
        )
    );

    schemas.add(
        GeneratorColumnSchema.makeContinuousUniform(
            "dimB",
            ValueType.FLOAT,
            false,
            1,
            null,
            210.0,
            250.0
        )
    );

    DataGenerator dataGenerator = new DataGenerator(schemas, 9999, 0, 0, 1000.0);
    for (int i = 0; i < 100; i++) {
      InputRow row = dataGenerator.nextRow();
      tracker.addRow(row);
    }

    tracker.printStuff();
    tracker.assertTotalRowSize("dimA", 100);
    tracker.assertTotalRowSize("dimB", 100);
  }

  @Test
  public void testIntervalBasedTimeGeneration()
  {
    List<GeneratorColumnSchema> schemas = new ArrayList<>();
    RowValueTracker tracker = new RowValueTracker();

    schemas.add(
        GeneratorColumnSchema.makeEnumeratedSequential(
            "dimB",
            ValueType.STRING,
            false,
            1,
            null,
            Arrays.asList("Hello", "World", "Foo", "Bar")
        )
    );

    DataGenerator dataGenerator = new DataGenerator(schemas, 9999, Intervals.utc(50000, 600000), 100);
    for (int i = 0; i < 100; i++) {
      InputRow row = dataGenerator.nextRow();
      tracker.addRow(row);
    }
    tracker.printStuff();
    tracker.assertTimeStamp("dimB", 50000, 600000);

    RowValueTracker tracker2 = new RowValueTracker();
    DataGenerator dataGenerator2 = new DataGenerator(schemas, 9999, Intervals.utc(50000, 50001), 100);
    for (int i = 0; i < 100; i++) {
      InputRow row = dataGenerator2.nextRow();
      tracker2.addRow(row);
    }
    tracker2.printStuff();
    tracker2.assertTimeStamp("dimB", 50000, 50001);
  }

  @Test
  public void testBasicSchemasAndGeneratorSchemaInfo()
  {
    GeneratorSchemaInfo basicSchema = GeneratorBasicSchemas.SCHEMA_MAP.get("basic");
    Assert.assertEquals(13, basicSchema.getColumnSchemas().size());
    Assert.assertEquals(6, basicSchema.getAggs().size());
    Assert.assertEquals(6, basicSchema.getAggsArray().length);
    Assert.assertNotNull(basicSchema.getDimensionsSpec());
    Assert.assertNotNull(basicSchema.getDataInterval());
    Assert.assertTrue(basicSchema.isWithRollup());
  }

  @Test
  public void testRealRoundingDistributionZeroGetters()
  {
    RealRoundingDistribution dist = new RealRoundingDistribution(new NormalDistribution());
    Assert.assertEquals(0, dist.getSupportLowerBound());
    Assert.assertEquals(0, dist.getSupportUpperBound());
    Assert.assertEquals(0, dist.getNumericalMean(), 0);
    Assert.assertEquals(0, dist.getNumericalVariance(), 0);
  }

  @Test
  public void testLazyZipf()
  {
    List<GeneratorColumnSchema> schemas = new ArrayList<>();
    RowValueTracker tracker = new RowValueTracker();

    schemas.add(
        GeneratorColumnSchema.makeLazyZipf(
            "dimA",
            ValueType.STRING,
            false,
            1,
            null,
            0,
            1220000,
            1.0
        )
    );

    schemas.add(
        GeneratorColumnSchema.makeLazyZipf(
            "dimB",
            ValueType.FLOAT,
            false,
            1,
            null,
            99990,
            99999,
            1.0
        )
    );

    schemas.add(
        GeneratorColumnSchema.makeLazyZipf(
            "dimC",
            ValueType.DOUBLE,
            false,
            1,
            null,
            0,
            100000,
            1.5
        )
    );
    schemas.add(
        GeneratorColumnSchema.makeLazyZipf(
            "dimD",
            ValueType.LONG,
            false,
            1,
            null,
            0,
            100000,
            1.5
        )
    );

    DataGenerator dataGenerator = new DataGenerator(schemas, 9999, 0, 0, 1000.0);
    for (int i = 0; i < 100000; i++) {
      InputRow row = dataGenerator.nextRow();
      Assert.assertNotNull(row);
      tracker.addRow(row);
    }

    tracker.printStuff();
    tracker.assertTotalRowSize("dimA", 100000);
    tracker.assertTotalRowSize("dimB", 100000);
    tracker.assertTotalRowSize("dimC", 100000);
    tracker.assertTotalRowSize("dimD", 100000);
  }

  private static class RowValuePropertyTracker
  {
    private Integer count;
    private DateTime timeStamp;

    public RowValuePropertyTracker(Integer count, DateTime timeStamp)
    {
      this.count = count;
      this.timeStamp = timeStamp;
    }

    public Integer getCount()
    {
      return count;
    }

    public DateTime getTimeStamp()
    {
      return timeStamp;
    }
  }

  private static class RowValueTracker
  {
    private Map<String, Map<Object, RowValuePropertyTracker>> dimensionMap;

    public RowValueTracker()
    {
      dimensionMap = new HashMap<>();
    }

    public void addRow(InputRow row)
    {
      for (String dim : row.getDimensions()) {
        if (dimensionMap.get(dim) == null) {
          dimensionMap.put(dim, new HashMap<Object, RowValuePropertyTracker>());
        }

        Map<Object, RowValuePropertyTracker> valueMap = dimensionMap.get(dim);
        Object dimVals = row.getRaw(dim);
        if (dimVals == null) {
          dimVals = Collections.singletonList(null);
        } else if (!(dimVals instanceof List)) {
          dimVals = Collections.singletonList(dimVals);
        }
        List<Object> dimValsList = (List) dimVals;

        for (Object val : dimValsList) {
          if (val == null) {
            val = "";
          }
          if (valueMap.get(val) == null) {
            valueMap.put(val, new RowValuePropertyTracker(0, row.getTimestamp()));
          }
          RowValuePropertyTracker property = valueMap.get(val);
          valueMap.put(val, new RowValuePropertyTracker(property.getCount() + 1, property.getTimeStamp()));
        }
      }
    }

    private void assertNullable(String dim, boolean expected)
    {
      String format = expected ? "%s dimension is nullable" : "%s dimension is not nullable";
      String message = String.format(Locale.US, format, dim);
      Map<Object, RowValuePropertyTracker> valueMap = dimensionMap.get(dim);
      Assert.assertEquals(message, expected, valueMap.containsKey(""));
    }

    private void assertTimeStamp(String dim, long startTime, long endTime)
    {
      Map<Object, RowValuePropertyTracker> valueMap = dimensionMap.get(dim);
      String message = String.format(
          Locale.US,
          "%s timeStamp value should be in the range of [%s, %s]",
          dim,
          startTime,
          endTime
      );
      for (Object val : valueMap.keySet()) {
        long timeStamp = valueMap.get(val).getTimeStamp().getMillis();
        Assert.assertTrue(message, timeStamp >= startTime && timeStamp <= endTime);
      }
    }

    private void assertTotalRowSize(String dim, int expected)
    {
      Map<Object, RowValuePropertyTracker> valueMap = dimensionMap.get(dim);
      int count = 0;
      String message = String.format(Locale.US, "%s dimension row size is not equal", dim);
      for (Object val : valueMap.keySet()) {
        count += valueMap.get(val).getCount();
      }
      Assert.assertEquals(message, expected, count);
    }

    public void printStuff()
    {
      for (String dim : dimensionMap.keySet()) {
        log.info("DIMENSION " + dim + "\n============");
        Map<Object, RowValuePropertyTracker> valueMap = dimensionMap.get(dim);

        List<Comparable> valList = new ArrayList<>();
        for (Object val : valueMap.keySet()) {
          valList.add((Comparable) val);
        }

        Collections.sort(valList);

        for (Comparable val : valList) {
          RowValuePropertyTracker property = valueMap.get(val);
          log.info(" VAL: " + val + " CNT: " + property.getCount() + " TIMESTAMP: " + property.getTimeStamp()
                                                                                              .getMillis());
        }
      }
    }
  }

  @Test
  public void testToList()
  {
    List<GeneratorColumnSchema> schemas = new ArrayList<>();
    RowValueTracker tracker = new RowValueTracker();

    schemas.add(
        GeneratorColumnSchema.makeSequential(
            "dimA",
            ValueType.STRING,
            false,
            1,
            null,
            10,
            20
        )
    );

    schemas.add(
        GeneratorColumnSchema.makeEnumeratedSequential(
            "dimB",
            ValueType.STRING,
            false,
            1,
            null,
            Arrays.asList("Hello", "World", "Foo", "Bar")
        )
    );

    schemas.add(
        GeneratorColumnSchema.makeSequential(
            "dimC",
            ValueType.STRING,
            false,
            1,
            0.50,
            30,
            40
        )
    );

    DataGenerator dataGenerator = new DataGenerator(schemas, 9999, 0, 0, 1000.0);
    List<InputRow> rows = dataGenerator.toList(100);
    Assert.assertEquals(100, rows.size());

    for (InputRow row : rows) {
      tracker.addRow(row);
    }
    tracker.printStuff();
  }

  @Test
  public void testToIndex()
  {
    List<GeneratorColumnSchema> schemas = new ArrayList<>();

    schemas.add(
        GeneratorColumnSchema.makeSequential(
            "dimA",
            ValueType.STRING,
            false,
            1,
            null,
            10,
            20
        )
    );

    schemas.add(
        GeneratorColumnSchema.makeEnumeratedSequential(
            "dimB",
            ValueType.STRING,
            false,
            1,
            null,
            Arrays.asList("Hello", "World", "Foo", "Bar")
        )
    );

    schemas.add(
        GeneratorColumnSchema.makeSequential(
            "dimC",
            ValueType.STRING,
            false,
            1,
            0.50,
            30,
            40
        )
    );

    DataGenerator dataGenerator = new DataGenerator(schemas, 9999, 0, 0, 1000.0);

    DimensionsSpec dimensions = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("dimA"),
            new StringDimensionSchema("dimB"),
            new StringDimensionSchema("dimC")
        )
    );
    AggregatorFactory[] metrics = {
        new CountAggregatorFactory("cnt")
    };
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withQueryGranularity(Granularities.MINUTE)
        .withDimensionsSpec(dimensions)
        .withMetrics(metrics)
        .withRollup(false)
        .build();

    IncrementalIndex index = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(1_000_000)
        .build();

    dataGenerator.addToIndex(index, 100);
    Assert.assertEquals(100, index.size());
  }
}
