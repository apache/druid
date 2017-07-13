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

package io.druid.benchmark.datagen;

import io.druid.segment.column.ValueType;
import org.apache.commons.math3.distribution.AbstractIntegerDistribution;
import org.apache.commons.math3.distribution.AbstractRealDistribution;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.util.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class BenchmarkColumnValueGenerator
{
  private final BenchmarkColumnSchema schema;
  private final long seed;

  private Serializable distribution;
  private Random simpleRng;

  public BenchmarkColumnValueGenerator(
      BenchmarkColumnSchema schema,
      long seed
  )
  {
    this.schema = schema;
    this.seed = seed;

    simpleRng = new Random(seed);
    initDistribution();
  }

  public Object generateRowValue()
  {
    Double nullProbability = schema.getNullProbability();
    int rowSize = schema.getRowSize();

    if (nullProbability != null) {
      double randDouble = simpleRng.nextDouble();
      if (randDouble <= nullProbability) {
        return null;
      }
    }

    if (rowSize == 1) {
      return generateSingleRowValue();
    } else {
      List<Object> rowVals = new ArrayList<>(rowSize);
      for (int i = 0; i < rowSize; i++) {
        rowVals.add(generateSingleRowValue());
      }
      return rowVals;
    }
  }

  public BenchmarkColumnSchema getSchema()
  {
    return schema;
  }

  public long getSeed()
  {
    return seed;
  }

  private Object generateSingleRowValue()
  {
    Object ret = null;
    ValueType type = schema.getType();

    if (distribution instanceof AbstractIntegerDistribution) {
      ret = ((AbstractIntegerDistribution) distribution).sample();
    } else if (distribution instanceof AbstractRealDistribution) {
      ret = ((AbstractRealDistribution) distribution).sample();
    } else if (distribution instanceof EnumeratedDistribution) {
      ret = ((EnumeratedDistribution) distribution).sample();
    }

    ret = convertType(ret, type);
    return ret;
  }

  private Object convertType(Object input, ValueType type)
  {
    if (input == null) {
      return null;
    }

    Object ret;
    switch (type) {
      case STRING:
        ret = input.toString();
        break;
      case LONG:
        if (input instanceof Number) {
          ret = ((Number) input).longValue();
        } else {
          ret = Long.parseLong(input.toString());
        }
        break;
      case FLOAT:
        if (input instanceof Number) {
          ret = ((Number) input).floatValue();
        } else {
          ret = Float.parseFloat(input.toString());
        }
        break;
      default:
        throw new UnsupportedOperationException("Unknown data type: " + type);
    }
    return ret;
  }

  private void initDistribution()
  {
    BenchmarkColumnSchema.ValueDistribution distributionType = schema.getDistributionType();
    ValueType type = schema.getType();
    List<Object> enumeratedValues = schema.getEnumeratedValues();
    List<Double> enumeratedProbabilities = schema.getEnumeratedProbabilities();
    List<Pair<Object, Double>> probabilities = new ArrayList<>();

    switch (distributionType) {
      case SEQUENTIAL:
        // not random, just cycle through numbers from start to end, or cycle through enumerated values if provided
        distribution = new SequentialDistribution(
            schema.getStartInt(),
            schema.getEndInt(),
            schema.getEnumeratedValues()
        );
        break;
      case UNIFORM:
        distribution = new UniformRealDistribution(schema.getStartDouble(), schema.getEndDouble());
        break;
      case DISCRETE_UNIFORM:
        if (enumeratedValues == null) {
          enumeratedValues = new ArrayList<>();
          for (int i = schema.getStartInt(); i < schema.getEndInt(); i++) {
            Object val = convertType(i, type);
            enumeratedValues.add(val);
          }
        }
        // give them all equal probability, the library will normalize probabilities to sum to 1.0
        for (int i = 0; i < enumeratedValues.size(); i++) {
          probabilities.add(new Pair<>(enumeratedValues.get(i), 0.1));
        }
        distribution = new EnumeratedTreeDistribution<>(probabilities);
        break;
      case NORMAL:
        distribution = new NormalDistribution(schema.getMean(), schema.getStandardDeviation());
        break;
      case ROUNDED_NORMAL:
        NormalDistribution normalDist = new NormalDistribution(schema.getMean(), schema.getStandardDeviation());
        distribution = new RealRoundingDistribution(normalDist);
        break;
      case ZIPF:
        int cardinality;
        if (enumeratedValues == null) {
          Integer startInt = schema.getStartInt();
          cardinality = schema.getEndInt() - startInt;
          ZipfDistribution zipf = new ZipfDistribution(cardinality, schema.getZipfExponent());
          for (int i = 0; i < cardinality; i++) {
            probabilities.add(new Pair<>((Object) (i + startInt), zipf.probability(i)));
          }
        } else {
          cardinality = enumeratedValues.size();
          ZipfDistribution zipf = new ZipfDistribution(enumeratedValues.size(), schema.getZipfExponent());
          for (int i = 0; i < cardinality; i++) {
            probabilities.add(new Pair<>(enumeratedValues.get(i), zipf.probability(i)));
          }
        }
        distribution = new EnumeratedTreeDistribution<>(probabilities);
        break;
      case ENUMERATED:
        for (int i = 0; i < enumeratedValues.size(); i++) {
          probabilities.add(new Pair<>(enumeratedValues.get(i), enumeratedProbabilities.get(i)));
        }
        distribution = new EnumeratedTreeDistribution<>(probabilities);
        break;

      default:
        throw new UnsupportedOperationException("Unknown distribution type: " + distributionType);
    }

    if (distribution instanceof AbstractIntegerDistribution) {
      ((AbstractIntegerDistribution) distribution).reseedRandomGenerator(seed);
    } else if (distribution instanceof AbstractRealDistribution) {
      ((AbstractRealDistribution) distribution).reseedRandomGenerator(seed);
    } else {
      ((EnumeratedDistribution) distribution).reseedRandomGenerator(seed);
    }
  }
}
