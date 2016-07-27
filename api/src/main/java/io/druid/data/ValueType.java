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

package io.druid.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;

import java.util.Comparator;

/**
 */
public enum ValueType
{
  FLOAT {
    @Override
    public Class classOfObject()
    {
      return Float.TYPE;
    }

    @Override
    public Comparator<Float> comparator()
    {
      return new Comparator<Float>()
      {
        @Override
        public int compare(Float o1, Float o2)
        {
          return Floats.compare(o1, o2);
        }
      };
    }
  },
  LONG {
    @Override
    public Class classOfObject()
    {
      return Long.TYPE;
    }

    @Override
    public Comparator<Long> comparator()
    {
      return new Comparator<Long>()
      {
        @Override
        public int compare(Long o1, Long o2)
        {
          return Longs.compare(o1, o2);
        }
      };
    }
  },
  DOUBLE {
    @Override
    public Class classOfObject()
    {
      return Double.TYPE;
    }

    @Override
    public Comparator<Double> comparator()
    {
      return new Comparator<Double>()
      {
        @Override
        public int compare(Double o1, Double o2)
        {
          return Doubles.compare(o1, o2);
        }
      };
    }
  },
  STRING {
    @Override
    public Class classOfObject()
    {
      return String.class;
    }

    public Comparator<String> comparator()
    {
      return Ordering.natural().nullsFirst();
    }
  },
  COMPLEX {
    @Override
    public Class classOfObject()
    {
      return Object.class;
    }

    @Override
    public Comparator comparator()
    {
      throw new UnsupportedOperationException();
    }
  };

  public abstract Class classOfObject();

  public abstract Comparator comparator();

  @JsonValue
  @Override
  public String toString()
  {
    return this.name().toUpperCase();
  }

  @JsonCreator
  public static ValueType fromString(String name)
  {
    return valueOf(name.toUpperCase());
  }

  public static ValueType of(String name)
  {
    try {
      return name == null ? COMPLEX : fromString(name);
    }
    catch (IllegalArgumentException e) {
      return COMPLEX;
    }
  }

  public static boolean isNumeric(ValueType type)
  {
    return type == DOUBLE || type == FLOAT || type == LONG;
  }
}
