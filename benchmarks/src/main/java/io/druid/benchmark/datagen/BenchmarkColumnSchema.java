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

import java.util.List;

public class BenchmarkColumnSchema
{
  /**
   * SEQUENTIAL:          Generate integer or enumerated values in sequence. Not random.
   *
   * DISCRETE_UNIFORM:    Discrete uniform distribution, generates integers or enumerated values.
   *
   * ROUNDED_NORMAL:      Discrete distribution that rounds sample values from an underlying normal
   *                      distribution
   *
   * ZIPF:                Discrete Zipf distribution.
   *                      Lower numbers have higher probability.
   *                      Can also generate Zipf distribution from a list of enumerated values.
   *
   * ENUMERATED:          Discrete distribution, generated from lists of values and associated probabilities.
   *
   * NORMAL:              Continuous normal distribution.
   *
   * UNIFORM:             Continuous uniform distribution.
   */
  public enum ValueDistribution
  {
    // discrete distributions
    SEQUENTIAL,
    DISCRETE_UNIFORM,
    ROUNDED_NORMAL,
    ZIPF,
    ENUMERATED,

    // continuous distributions
    UNIFORM,
    NORMAL
  }

  /**
   * Generate values according to this distribution.
   */
  private ValueDistribution distributionType;

  /**
   * Name of the column.
   */
  private String name;

  /**
   * Value type of this column.
   */
  private ValueType type;

  /**
   * Is this column a metric or dimension?
   */
  private boolean isMetric;

  /**
   * Controls how many values are generated per row (use > 1 for multi-value dimensions)
   */
  private int rowSize;

  /**
   * Probability that a null row will be generated instead of a row with values sampled from the distribution.
   */
  private final Double nullProbability;

  /**
   * When used in discrete distributions, the set of possible values to be generated.
   */
  private List<Object> enumeratedValues;

  /**
   * When using ENUMERATED distribution, the probabilities associated with the set of values to be generated.
   * The probabilities in this list must follow the same order as those in enumeratedValues.
   * Probabilities do not need to sum to 1.0, they will be automatically normalized.
   */
  private List<Double> enumeratedProbabilities;

  /**
   * Range of integer values to generate in ZIPF and DISCRETE_NORMAL distributions.
   */
  private Integer startInt;
  private Integer endInt;

  /**
   * Range of double values to generate in NORMAL distribution.
   */
  private Double startDouble;
  private Double endDouble;

  /**
   * Exponent for the ZIPF distribution.
   */
  private Double zipfExponent;

  /**
   * Mean and standard deviation for the NORMAL and ROUNDED_NORMAL distributions.
   */
  private Double mean;
  private Double standardDeviation;

  private BenchmarkColumnSchema(
      String name,
      ValueType type,
      boolean isMetric,
      int rowSize,
      Double nullProbability,
      ValueDistribution distributionType
  )
  {
    this.name = name;
    this.type = type;
    this.isMetric = isMetric;
    this.distributionType = distributionType;
    this.rowSize = rowSize;
    this.nullProbability = nullProbability;
  }

  public BenchmarkColumnValueGenerator makeGenerator(long seed)
  {
    return new BenchmarkColumnValueGenerator(this, seed);
  }

  public String getName()
  {
    return name;
  }

  public Double getNullProbability()
  {
    return nullProbability;
  }

  public ValueType getType()
  {
    return type;
  }

  public boolean isMetric()
  {
    return isMetric;
  }

  public ValueDistribution getDistributionType()
  {
    return distributionType;
  }

  public int getRowSize()
  {
    return rowSize;
  }

  public List<Object> getEnumeratedValues()
  {
    return enumeratedValues;
  }

  public List<Double> getEnumeratedProbabilities()
  {
    return enumeratedProbabilities;
  }

  public Integer getStartInt()
  {
    return startInt;
  }

  public Integer getEndInt()
  {
    return endInt;
  }

  public Double getStartDouble()
  {
    return startDouble;
  }

  public Double getEndDouble()
  {
    return endDouble;
  }

  public Double getZipfExponent()
  {
    return zipfExponent;
  }

  public Double getMean()
  {
    return mean;
  }

  public Double getStandardDeviation()
  {
    return standardDeviation;
  }

  public static BenchmarkColumnSchema makeSequential(
      String name,
      ValueType type,
      boolean isMetric,
      int rowSize,
      Double nullProbability,
      int startInt,
      int endInt
  )
  {
    BenchmarkColumnSchema schema = new BenchmarkColumnSchema(
        name,
        type,
        isMetric,
        rowSize,
        nullProbability,
        ValueDistribution.SEQUENTIAL
    );
    schema.startInt = startInt;
    schema.endInt = endInt;
    return schema;
  }

  public static BenchmarkColumnSchema makeEnumeratedSequential(
      String name,
      ValueType type,
      boolean isMetric,
      int rowSize,
      Double nullProbability,
      List<Object> enumeratedValues
  )
  {
    BenchmarkColumnSchema schema = new BenchmarkColumnSchema(
        name,
        type,
        isMetric,
        rowSize,
        nullProbability,
        ValueDistribution.SEQUENTIAL
    );
    schema.enumeratedValues = enumeratedValues;
    return schema;
  }

  public static BenchmarkColumnSchema makeDiscreteUniform(
      String name,
      ValueType type,
      boolean isMetric,
      int rowSize,
      Double nullProbability,
      int startInt,
      int endInt
  )
  {
    BenchmarkColumnSchema schema = new BenchmarkColumnSchema(
        name,
        type,
        isMetric,
        rowSize,
        nullProbability,
        ValueDistribution.DISCRETE_UNIFORM
    );
    schema.startInt = startInt;
    schema.endInt = endInt;
    return schema;
  }

  public static BenchmarkColumnSchema makeEnumeratedDiscreteUniform(
      String name,
      ValueType type,
      boolean isMetric,
      int rowSize,
      Double nullProbability,
      List<Object> enumeratedValues
  )
  {
    BenchmarkColumnSchema schema = new BenchmarkColumnSchema(
        name,
        type,
        isMetric,
        rowSize,
        nullProbability,
        ValueDistribution.DISCRETE_UNIFORM
    );
    schema.enumeratedValues = enumeratedValues;
    return schema;
  }

  public static BenchmarkColumnSchema makeContinuousUniform(
      String name,
      ValueType type,
      boolean isMetric,
      int rowSize,
      Double nullProbability,
      double startDouble,
      double endDouble
  )
  {
    BenchmarkColumnSchema schema = new BenchmarkColumnSchema(
        name,
        type,
        isMetric,
        rowSize,
        nullProbability,
        ValueDistribution.UNIFORM
    );
    schema.startDouble = startDouble;
    schema.endDouble = endDouble;
    return schema;
  }

  public static BenchmarkColumnSchema makeNormal(
      String name,
      ValueType type,
      boolean isMetric,
      int rowSize,
      Double nullProbability,
      Double mean,
      Double standardDeviation,
      boolean useRounding
  )
  {
    BenchmarkColumnSchema schema = new BenchmarkColumnSchema(
        name,
        type,
        isMetric,
        rowSize,
        nullProbability,
        useRounding ? ValueDistribution.ROUNDED_NORMAL : ValueDistribution.NORMAL
    );
    schema.mean = mean;
    schema.standardDeviation = standardDeviation;
    return schema;
  }

  public static BenchmarkColumnSchema makeZipf(
      String name,
      ValueType type,
      boolean isMetric,
      int rowSize,
      Double nullProbability,
      int startInt,
      int endInt,
      Double zipfExponent
  )
  {
    BenchmarkColumnSchema schema = new BenchmarkColumnSchema(
        name,
        type,
        isMetric,
        rowSize,
        nullProbability,
        ValueDistribution.ZIPF
    );
    schema.startInt = startInt;
    schema.endInt = endInt;
    schema.zipfExponent = zipfExponent;
    return schema;
  }

  public static BenchmarkColumnSchema makeEnumeratedZipf(
      String name,
      ValueType type,
      boolean isMetric,
      int rowSize,
      Double nullProbability,
      List<Object> enumeratedValues,
      Double zipfExponent
  )
  {
    BenchmarkColumnSchema schema = new BenchmarkColumnSchema(
        name,
        type,
        isMetric,
        rowSize,
        nullProbability,
        ValueDistribution.ZIPF
    );
    schema.enumeratedValues = enumeratedValues;
    schema.zipfExponent = zipfExponent;
    return schema;
  }

  public static BenchmarkColumnSchema makeEnumerated(
      String name,
      ValueType type,
      boolean isMetric,
      int rowSize,
      Double nullProbability,
      List<Object> enumeratedValues,
      List<Double> enumeratedProbabilities
  )
  {
    BenchmarkColumnSchema schema = new BenchmarkColumnSchema(
        name,
        type,
        isMetric,
        rowSize,
        nullProbability,
        ValueDistribution.ENUMERATED
    );
    schema.enumeratedValues = enumeratedValues;
    schema.enumeratedProbabilities = enumeratedProbabilities;
    return schema;
  }
}
