/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.benchmark;

import io.druid.benchmark.datagen.BenchmarkColumnSchema;
import io.druid.benchmark.datagen.BenchmarkDataGenerator;
import io.druid.data.input.InputRow;
import io.druid.segment.column.ValueType;
import org.joda.time.Interval;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Doesn't assert behavior right now, just generates rows and prints out some distribution numbers
public class BenchmarkDataGeneratorTest
{
  @Test
  public void testSequential() throws Exception
  {
    List<BenchmarkColumnSchema> schemas = new ArrayList<>();
    RowValueTracker tracker = new RowValueTracker();

    schemas.add(
        BenchmarkColumnSchema.makeSequential(
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
        BenchmarkColumnSchema.makeEnumeratedSequential(
            "dimB",
            ValueType.STRING,
            false,
            1,
            null,
            Arrays.<Object>asList("Hello", "World", "Foo", "Bar")
        )
    );

    schemas.add(
        BenchmarkColumnSchema.makeSequential(
            "dimC",
            ValueType.STRING,
            false,
            1,
            0.50,
            30,
            40
        )
    );

    BenchmarkDataGenerator dataGenerator = new BenchmarkDataGenerator(schemas, 9999, 0, 0, 1000.0);
    for (int i = 0; i < 100; i++) {
      InputRow row = dataGenerator.nextRow();
      //System.out.println("S-ROW: " + row);
      tracker.addRow(row);
    }
    tracker.printStuff();
  }

  @Test
  public void testDiscreteUniform() throws Exception
  {
    List<BenchmarkColumnSchema> schemas = new ArrayList<>();
    RowValueTracker tracker = new RowValueTracker();

    schemas.add(
        BenchmarkColumnSchema.makeDiscreteUniform(
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
        BenchmarkColumnSchema.makeEnumeratedDiscreteUniform(
            "dimB",
            ValueType.STRING,
            false,
            4,
            null,
            Arrays.<Object>asList("Hello", "World", "Foo", "Bar")
        )
    );

    schemas.add(
        BenchmarkColumnSchema.makeDiscreteUniform(
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
        BenchmarkColumnSchema.makeDiscreteUniform(
            "dimD",
            ValueType.FLOAT,
            false,
            1,
            null,
            100,
            120
        )
    );

    BenchmarkDataGenerator dataGenerator = new BenchmarkDataGenerator(schemas, 9999, 0, 0, 1000.0);
    for (int i = 0; i < 100; i++) {
      InputRow row = dataGenerator.nextRow();
      //System.out.println("U-ROW: " + row);

      tracker.addRow(row);
    }

    tracker.printStuff();
  }


  @Test
  public void testRoundedNormal() throws Exception
  {
    List<BenchmarkColumnSchema> schemas = new ArrayList<>();
    RowValueTracker tracker = new RowValueTracker();

    schemas.add(
        BenchmarkColumnSchema.makeNormal(
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
        BenchmarkColumnSchema.makeNormal(
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

    BenchmarkDataGenerator dataGenerator = new BenchmarkDataGenerator(schemas, 9999, 0, 0, 1000.0);
    for (int i = 0; i < 1000000; i++) {
      InputRow row = dataGenerator.nextRow();
      //System.out.println("N-ROW: " + row);

      tracker.addRow(row);
    }

    tracker.printStuff();
  }

  @Test
  public void testZipf() throws Exception
  {
    List<BenchmarkColumnSchema> schemas = new ArrayList<>();
    RowValueTracker tracker = new RowValueTracker();

    schemas.add(
        BenchmarkColumnSchema.makeZipf(
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
        BenchmarkColumnSchema.makeZipf(
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
        BenchmarkColumnSchema.makeEnumeratedZipf(
            "dimC",
            ValueType.STRING,
            false,
            1,
            null,
            Arrays.<Object>asList("1-Hello", "2-World", "3-Foo", "4-Bar", "5-BA5EBA11", "6-Rocky", "7-Mango", "8-Contango"),
            1.0
        )
    );

    BenchmarkDataGenerator dataGenerator = new BenchmarkDataGenerator(schemas, 9999, 0, 0, 1000.0);
    for (int i = 0; i < 100; i++) {
      InputRow row = dataGenerator.nextRow();
      //System.out.println("Z-ROW: " + row);

      tracker.addRow(row);
    }

    tracker.printStuff();
  }

  @Test
  public void testEnumerated() throws Exception
  {
    List<BenchmarkColumnSchema> schemas = new ArrayList<>();
    RowValueTracker tracker = new RowValueTracker();

    schemas.add(
        BenchmarkColumnSchema.makeEnumerated(
            "dimA",
            ValueType.STRING,
            false,
            1,
            null,
            Arrays.<Object>asList("Hello", "World", "Foo", "Bar"),
            Arrays.<Double>asList(0.5, 0.25, 0.15, 0.10)
        )
    );

    BenchmarkDataGenerator dataGenerator = new BenchmarkDataGenerator(schemas, 9999, 0, 0, 1000.0);
    for (int i = 0; i < 10000; i++) {
      InputRow row = dataGenerator.nextRow();
      //System.out.println("Z-ROW: " + row);

      tracker.addRow(row);
    }

    tracker.printStuff();
  }

  @Test
  public void testNormal() throws Exception
  {
    List<BenchmarkColumnSchema> schemas = new ArrayList<>();
    RowValueTracker tracker = new RowValueTracker();

    schemas.add(
        BenchmarkColumnSchema.makeNormal(
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
        BenchmarkColumnSchema.makeNormal(
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

    BenchmarkDataGenerator dataGenerator = new BenchmarkDataGenerator(schemas, 9999, 0, 0, 1000.0);
    for (int i = 0; i < 100; i++) {
      InputRow row = dataGenerator.nextRow();
      //System.out.println("N-ROW: " + row);

      tracker.addRow(row);
    }

    tracker.printStuff();
  }

  @Test
  public void testRealUniform() throws Exception
  {
    List<BenchmarkColumnSchema> schemas = new ArrayList<>();
    RowValueTracker tracker = new RowValueTracker();

    schemas.add(
        BenchmarkColumnSchema.makeContinuousUniform(
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
        BenchmarkColumnSchema.makeContinuousUniform(
            "dimB",
            ValueType.FLOAT,
            false,
            1,
            null,
            210.0,
            250.0
        )
    );

    BenchmarkDataGenerator dataGenerator = new BenchmarkDataGenerator(schemas, 9999, 0, 0, 1000.0);
    for (int i = 0; i < 100; i++) {
      InputRow row = dataGenerator.nextRow();
      //System.out.println("U-ROW: " + row);

      tracker.addRow(row);
    }

    tracker.printStuff();
  }

  @Test
  public void testIntervalBasedTimeGeneration() throws Exception
  {
    List<BenchmarkColumnSchema> schemas = new ArrayList<>();

    schemas.add(
        BenchmarkColumnSchema.makeEnumeratedSequential(
            "dimB",
            ValueType.STRING,
            false,
            1,
            null,
            Arrays.<Object>asList("Hello", "World", "Foo", "Bar")
        )
    );

    BenchmarkDataGenerator dataGenerator = new BenchmarkDataGenerator(schemas, 9999, new Interval(50000, 600000), 100);
    for (int i = 0; i < 100; i++) {
      InputRow row = dataGenerator.nextRow();
      //System.out.println("S-ROW: " + row);
    }

    BenchmarkDataGenerator dataGenerator2 = new BenchmarkDataGenerator(schemas, 9999, new Interval(50000, 50001), 100);
    for (int i = 0; i < 100; i++) {
      InputRow row = dataGenerator2.nextRow();
      //System.out.println("S2-ROW: " + row);
    }
  }


  private static class RowValueTracker
  {
    private Map<String, Map<Object, Integer>> dimensionMap;

    public RowValueTracker()
    {
      dimensionMap = new HashMap<>();
    }

    public void addRow(InputRow row)
    {
      for (String dim : row.getDimensions()) {
        if (dimensionMap.get(dim) == null) {
          dimensionMap.put(dim, new HashMap<Object, Integer>());
        }

        Map<Object, Integer> valueMap = dimensionMap.get(dim);
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
            valueMap.put(val, 0);
          }
          valueMap.put(val, valueMap.get(val) + 1);
        }
      }
    }


    public void printStuff()
    {
      System.out.println();
      for (String dim : dimensionMap.keySet()) {
        System.out.println("DIMENSION " + dim + "\n============");
        Map<Object, Integer> valueMap = dimensionMap.get(dim);

        List<Comparable> valList = new ArrayList<>();
        for (Object val : valueMap.keySet()) {
          valList.add((Comparable) val);
        }

        Collections.sort(valList);

        for (Comparable val : valList) {
          System.out.println(" VAL: " + val.toString() + " CNT: " + valueMap.get(val));
        }
        System.out.println();
      }
    }
  }
}
