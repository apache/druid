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

//CHECKSTYLE.OFF: PackageName - Must be in Calcite

package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptUtil;

import java.util.Objects;

/**
 * Druid's extension to {@link VolcanoCost}. The difference between the two is in
 * comparing two costs. Druid's cost model gives most weightage to rowCount, then to cpuCost and then lastly ioCost.
 */
public class DruidVolcanoCost implements RelOptCost
{

  static final DruidVolcanoCost INFINITY = new DruidVolcanoCost(
      Double.POSITIVE_INFINITY,
      Double.POSITIVE_INFINITY,
      Double.POSITIVE_INFINITY
  )
  {
    @Override
    public String toString()
    {
      return "{inf}";
    }
  };

  //CHECKSTYLE.OFF: Regexp
  static final DruidVolcanoCost HUGE = new DruidVolcanoCost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE) {
    @Override
    public String toString()
    {
      return "{huge}";
    }
  };
  //CHECKSTYLE.ON: Regexp

  static final DruidVolcanoCost ZERO =
      new DruidVolcanoCost(0.0, 0.0, 0.0)
      {
        @Override
        public String toString()
        {
          return "{0}";
        }
      };

  static final DruidVolcanoCost TINY =
      new DruidVolcanoCost(1.0, 1.0, 0.0)
      {
        @Override
        public String toString()
        {
          return "{tiny}";
        }
      };

  public static final RelOptCostFactory FACTORY = new Factory();

  final double cpu;
  final double io;
  final double rowCount;

  DruidVolcanoCost(double rowCount, double cpu, double io)
  {
    this.rowCount = rowCount;
    this.cpu = cpu;
    this.io = io;
  }

  @Override
  public double getCpu()
  {
    return cpu;
  }

  @Override
  public boolean isInfinite()
  {
    return (this == INFINITY)
           || (this.rowCount == Double.POSITIVE_INFINITY)
           || (this.cpu == Double.POSITIVE_INFINITY)
           || (this.io == Double.POSITIVE_INFINITY);
  }

  @Override
  public double getIo()
  {
    return io;
  }

  @Override
  public boolean isLe(RelOptCost other)
  {
    DruidVolcanoCost that = (DruidVolcanoCost) other;
    return (this == that)
           || ((this.rowCount < that.rowCount)
               || (this.rowCount == that.rowCount && this.cpu < that.cpu)
               || (this.rowCount == that.rowCount && this.cpu == that.cpu && this.io <= that.io));
  }

  @Override
  public boolean isLt(RelOptCost other)
  {
    return isLe(other) && !equals(other);
  }

  @Override
  public double getRows()
  {
    return rowCount;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(rowCount, cpu, io);
  }

  @Override
  public boolean equals(RelOptCost other)
  {
    return this == other
           || other instanceof DruidVolcanoCost
              && (this.rowCount == ((DruidVolcanoCost) other).rowCount)
              && (this.cpu == ((DruidVolcanoCost) other).cpu)
              && (this.io == ((DruidVolcanoCost) other).io);
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj instanceof DruidVolcanoCost) {
      return equals((DruidVolcanoCost) obj);
    }
    return false;
  }

  @Override
  public boolean isEqWithEpsilon(RelOptCost other)
  {
    if (!(other instanceof DruidVolcanoCost)) {
      return false;
    }
    DruidVolcanoCost that = (DruidVolcanoCost) other;
    return (this == that)
           || ((Math.abs(this.rowCount - that.rowCount) < RelOptUtil.EPSILON)
               && (Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON)
               && (Math.abs(this.io - that.io) < RelOptUtil.EPSILON));
  }

  @Override
  public RelOptCost minus(RelOptCost other)
  {
    if (this == INFINITY) {
      return this;
    }
    DruidVolcanoCost that = (DruidVolcanoCost) other;
    return new DruidVolcanoCost(
        this.rowCount - that.rowCount,
        this.cpu - that.cpu,
        this.io - that.io
    );
  }

  @Override
  public RelOptCost multiplyBy(double factor)
  {
    if (this == INFINITY) {
      return this;
    }
    return new DruidVolcanoCost(rowCount * factor, cpu * factor, io * factor);
  }

  @Override
  public double divideBy(RelOptCost cost)
  {
    // Compute the geometric average of the ratios of all of the factors
    // which are non-zero and finite.
    DruidVolcanoCost that = (DruidVolcanoCost) cost;
    double d = 1;
    double n = 0;
    if ((this.rowCount != 0)
        && !Double.isInfinite(this.rowCount)
        && (that.rowCount != 0)
        && !Double.isInfinite(that.rowCount)) {
      d *= this.rowCount / that.rowCount;
      ++n;
    }
    if ((this.cpu != 0)
        && !Double.isInfinite(this.cpu)
        && (that.cpu != 0)
        && !Double.isInfinite(that.cpu)) {
      d *= this.cpu / that.cpu;
      ++n;
    }
    if ((this.io != 0)
        && !Double.isInfinite(this.io)
        && (that.io != 0)
        && !Double.isInfinite(that.io)) {
      d *= this.io / that.io;
      ++n;
    }
    if (n == 0) {
      return 1.0;
    }
    return Math.pow(d, 1 / n);
  }

  @Override
  public RelOptCost plus(RelOptCost other)
  {
    DruidVolcanoCost that = (DruidVolcanoCost) other;
    if ((this == INFINITY) || (that == INFINITY)) {
      return INFINITY;
    }
    return new DruidVolcanoCost(
        this.rowCount + that.rowCount,
        this.cpu + that.cpu,
        this.io + that.io
    );
  }

  @Override
  public String toString()
  {
    return "{" + rowCount + " rows, " + cpu + " cpu, " + io + " io}";
  }

  /**
   * Implementation of {@link RelOptCostFactory}
   * that creates {@link DruidVolcanoCost}s.
   */
  public static class Factory implements RelOptCostFactory
  {
    @Override
    public RelOptCost makeCost(double dRows, double dCpu, double dIo)
    {
      return new DruidVolcanoCost(dRows, dCpu, dIo);
    }

    @Override
    public RelOptCost makeHugeCost()
    {
      return DruidVolcanoCost.HUGE;
    }

    @Override
    public RelOptCost makeInfiniteCost()
    {
      return DruidVolcanoCost.INFINITY;
    }

    @Override
    public RelOptCost makeTinyCost()
    {
      return DruidVolcanoCost.TINY;
    }

    @Override
    public RelOptCost makeZeroCost()
    {
      return DruidVolcanoCost.ZERO;
    }
  }
}
