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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.column.ValueType;

import java.util.List;
import java.util.Objects;

public class GeneratorColumnSchema
{

  /**
   * SEQUENTIAL:            Generate integer or enumerated values in sequence. Not random.
   *
   * DISCRETE_UNIFORM:      Discrete uniform distribution, generates integers or enumerated values.
   *
   * ROUNDED_NORMAL:        Discrete distribution that rounds sample values from an underlying normal
   *                        distribution
   *
   * ZIPF:                  Discrete Zipf distribution.
   *                        Lower numbers have higher probability.
   *                        Can also generate Zipf distribution from a list of enumerated values.
   *
   * LAZY_ZIPF:             ZIPF but lazy evaluated for large cardinalities
   *
   * LAZY_DISCRETE_UNIFORM: DISCRETE_UNIFORM but lazy evaluated for large cardinalities
   *
   * ENUMERATED:            Discrete distribution, generated from lists of values and associated probabilities.
   *
   * NORMAL:                Continuous normal distribution.
   *
   * UNIFORM:               Continuous uniform distribution.
   */
  public enum ValueDistribution
  {
    // discrete distributions
    SEQUENTIAL,
    DISCRETE_UNIFORM,
    ROUNDED_NORMAL,
    ZIPF,
    ENUMERATED,
    LAZY_ZIPF,
    LAZY_DISCRETE_UNIFORM,

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

  @JsonCreator
  public GeneratorColumnSchema(
      @JsonProperty("name") String name,
      @JsonProperty("type") ValueType type,
      @JsonProperty("isMetric") boolean isMetric,
      @JsonProperty("rowSize") int rowSize,
      @JsonProperty("nullProbability") Double nullProbability,
      @JsonProperty("distributionType") ValueDistribution distributionType,
      @JsonProperty("enumeratedValues") List<Object> enumeratedValues,
      @JsonProperty("enumeratedProbabilities") List<Double> enumeratedProbabilities,
      @JsonProperty("startInt") Integer startInt,
      @JsonProperty("endInt") Integer endInt,
      @JsonProperty("startDouble") Double startDouble,
      @JsonProperty("endDouble") Double endDouble,
      @JsonProperty("zipfExponent") Double zipfExponent,
      @JsonProperty("mean") Double mean,
      @JsonProperty("standardDeviation") Double standardDeviation
  )
  {
    this.name = name;
    this.type = type;
    this.isMetric = isMetric;
    this.distributionType = distributionType;
    this.rowSize = rowSize;
    this.nullProbability = nullProbability;
    this.enumeratedValues = enumeratedValues;
    this.enumeratedProbabilities = enumeratedProbabilities;
    this.startInt = startInt;
    this.endInt = endInt;
    this.startDouble = startDouble;
    this.endDouble = endDouble;
    this.zipfExponent = zipfExponent;
    this.mean = mean;
    this.standardDeviation = standardDeviation;
  }

  private GeneratorColumnSchema(
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

  public ColumnValueGenerator makeGenerator(long seed)
  {
    return new ColumnValueGenerator(this, seed);
  }

  @JsonIgnore
  public DimensionSchema getDimensionSchema()
  {
    switch (type) {
      case LONG:
        return new LongDimensionSchema(name);
      case FLOAT:
        return new FloatDimensionSchema(name);
      case DOUBLE:
        return new DoubleDimensionSchema(name);
      case STRING:
        return new StringDimensionSchema(name);
      default:
        throw new IAE("unable to make dimension schema for %s", type);
    }
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public Double getNullProbability()
  {
    return nullProbability;
  }

  @JsonProperty
  public ValueType getType()
  {
    return type;
  }

  @JsonProperty
  public boolean isMetric()
  {
    return isMetric;
  }

  @JsonProperty
  public ValueDistribution getDistributionType()
  {
    return distributionType;
  }

  @JsonProperty
  public int getRowSize()
  {
    return rowSize;
  }

  @JsonProperty
  public List<Object> getEnumeratedValues()
  {
    return enumeratedValues;
  }

  @JsonProperty
  public List<Double> getEnumeratedProbabilities()
  {
    return enumeratedProbabilities;
  }

  @JsonProperty
  public Integer getStartInt()
  {
    return startInt;
  }

  @JsonProperty
  public Integer getEndInt()
  {
    return endInt;
  }

  @JsonProperty
  public Double getStartDouble()
  {
    return startDouble;
  }

  @JsonProperty
  public Double getEndDouble()
  {
    return endDouble;
  }

  @JsonProperty
  public Double getZipfExponent()
  {
    return zipfExponent;
  }

  @JsonProperty
  public Double getMean()
  {
    return mean;
  }

  @JsonProperty
  public Double getStandardDeviation()
  {
    return standardDeviation;
  }

  public static GeneratorColumnSchema makeSequential(
      String name,
      ValueType type,
      boolean isMetric,
      int rowSize,
      Double nullProbability,
      int startInt,
      int endInt
  )
  {
    GeneratorColumnSchema schema = new GeneratorColumnSchema(
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

  public static GeneratorColumnSchema makeEnumeratedSequential(
      String name,
      ValueType type,
      boolean isMetric,
      int rowSize,
      Double nullProbability,
      List<Object> enumeratedValues
  )
  {
    GeneratorColumnSchema schema = new GeneratorColumnSchema(
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

  public static GeneratorColumnSchema makeDiscreteUniform(
      String name,
      ValueType type,
      boolean isMetric,
      int rowSize,
      Double nullProbability,
      int startInt,
      int endInt
  )
  {
    GeneratorColumnSchema schema = new GeneratorColumnSchema(
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

  public static GeneratorColumnSchema makeEnumeratedDiscreteUniform(
      String name,
      ValueType type,
      boolean isMetric,
      int rowSize,
      Double nullProbability,
      List<Object> enumeratedValues
  )
  {
    GeneratorColumnSchema schema = new GeneratorColumnSchema(
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

  public static GeneratorColumnSchema makeLazyDiscreteUniform(
      String name,
      ValueType type,
      boolean isMetric,
      int rowSize,
      Double nullProbability,
      int startInt,
      int endInt
  )
  {
    GeneratorColumnSchema schema = new GeneratorColumnSchema(
        name,
        type,
        isMetric,
        rowSize,
        nullProbability,
        ValueDistribution.LAZY_DISCRETE_UNIFORM
    );
    schema.startInt = startInt;
    schema.endInt = endInt;
    return schema;
  }


  public static GeneratorColumnSchema makeContinuousUniform(
      String name,
      ValueType type,
      boolean isMetric,
      int rowSize,
      Double nullProbability,
      double startDouble,
      double endDouble
  )
  {
    GeneratorColumnSchema schema = new GeneratorColumnSchema(
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

  public static GeneratorColumnSchema makeNormal(
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
    GeneratorColumnSchema schema = new GeneratorColumnSchema(
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

  public static GeneratorColumnSchema makeZipf(
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
    GeneratorColumnSchema schema = new GeneratorColumnSchema(
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

  public static GeneratorColumnSchema makeLazyZipf(
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
    GeneratorColumnSchema schema = new GeneratorColumnSchema(
        name,
        type,
        isMetric,
        rowSize,
        nullProbability,
        ValueDistribution.LAZY_ZIPF
    );
    schema.startInt = startInt;
    schema.endInt = endInt;
    schema.zipfExponent = zipfExponent;
    return schema;
  }

  public static GeneratorColumnSchema makeEnumeratedZipf(
      String name,
      ValueType type,
      boolean isMetric,
      int rowSize,
      Double nullProbability,
      List<Object> enumeratedValues,
      Double zipfExponent
  )
  {
    GeneratorColumnSchema schema = new GeneratorColumnSchema(
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

  public static GeneratorColumnSchema makeEnumerated(
      String name,
      ValueType type,
      boolean isMetric,
      int rowSize,
      Double nullProbability,
      List<Object> enumeratedValues,
      List<Double> enumeratedProbabilities
  )
  {
    GeneratorColumnSchema schema = new GeneratorColumnSchema(
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

  @Override
  public String toString()
  {
    return "BenchmarkColumnSchema{" +
           "distributionType=" + distributionType +
           ", name='" + name + '\'' +
           ", type=" + type +
           ", isMetric=" + isMetric +
           ", rowSize=" + rowSize +
           ", nullProbability=" + nullProbability +
           ", enumeratedValues=" + enumeratedValues +
           ", enumeratedProbabilities=" + enumeratedProbabilities +
           ", startInt=" + startInt +
           ", endInt=" + endInt +
           ", startDouble=" + startDouble +
           ", endDouble=" + endDouble +
           ", zipfExponent=" + zipfExponent +
           ", mean=" + mean +
           ", standardDeviation=" + standardDeviation +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GeneratorColumnSchema that = (GeneratorColumnSchema) o;
    return isMetric == that.isMetric &&
           rowSize == that.rowSize &&
           distributionType == that.distributionType &&
           name.equals(that.name) &&
           type == that.type &&
           Objects.equals(nullProbability, that.nullProbability) &&
           Objects.equals(enumeratedValues, that.enumeratedValues) &&
           Objects.equals(enumeratedProbabilities, that.enumeratedProbabilities) &&
           Objects.equals(startInt, that.startInt) &&
           Objects.equals(endInt, that.endInt) &&
           Objects.equals(startDouble, that.startDouble) &&
           Objects.equals(endDouble, that.endDouble) &&
           Objects.equals(zipfExponent, that.zipfExponent) &&
           Objects.equals(mean, that.mean) &&
           Objects.equals(standardDeviation, that.standardDeviation);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        distributionType,
        name,
        type,
        isMetric,
        rowSize,
        nullProbability,
        enumeratedValues,
        enumeratedProbabilities,
        startInt,
        endInt,
        startDouble,
        endDouble,
        zipfExponent,
        mean,
        standardDeviation
    );
  }
}
