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

package org.apache.druid.math.expr;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SettableVectorInputBinding implements Expr.VectorInputBinding
{
  private final Map<String, boolean[]> nulls;
  private final Map<String, long[]> longs;
  private final Map<String, double[]> doubles;
  private final Map<String, Object[]> objects;
  private final Map<String, ExpressionType> types;

  private final int vectorSize;

  private int id = 0;

  public SettableVectorInputBinding(int vectorSize)
  {
    this.nulls = new HashMap<>();
    this.longs = new HashMap<>();
    this.doubles = new HashMap<>();
    this.objects = new HashMap<>();
    this.types = new HashMap<>();
    this.vectorSize = vectorSize;
  }

  public SettableVectorInputBinding addBinding(String name, ExpressionType type, boolean[] nulls)
  {
    this.nulls.put(name, nulls);
    this.types.put(name, type);
    return this;
  }

  public SettableVectorInputBinding addLong(String name, long[] longs)
  {
    return addLong(name, longs, new boolean[longs.length]);
  }

  public SettableVectorInputBinding addLong(String name, long[] longs, boolean[] nulls)
  {
    assert longs.length == vectorSize;
    this.longs.put(name, longs);
    this.doubles.put(name, Arrays.stream(longs).asDoubleStream().toArray());
    return addBinding(name, ExpressionType.LONG, nulls);
  }

  public SettableVectorInputBinding addDouble(String name, double[] doubles)
  {
    return addDouble(name, doubles, new boolean[doubles.length]);
  }

  public SettableVectorInputBinding addDouble(String name, double[] doubles, boolean[] nulls)
  {
    assert doubles.length == vectorSize;
    this.doubles.put(name, doubles);
    this.longs.put(name, Arrays.stream(doubles).mapToLong(x -> (long) x).toArray());
    return addBinding(name, ExpressionType.DOUBLE, nulls);
  }

  public SettableVectorInputBinding addString(String name, Object[] strings)
  {
    assert strings.length == vectorSize;
    this.objects.put(name, strings);
    return addBinding(name, ExpressionType.STRING, new boolean[strings.length]);
  }

  @Override
  public Object[] getObjectVector(String name)
  {
    return objects.getOrDefault(name, new Object[getCurrentVectorSize()]);
  }

  @Override
  public ExpressionType getType(String name)
  {
    return types.get(name);
  }

  @Override
  public long[] getLongVector(String name)
  {
    return longs.getOrDefault(name, new long[getCurrentVectorSize()]);
  }

  @Override
  public double[] getDoubleVector(String name)
  {
    return doubles.getOrDefault(name, new double[getCurrentVectorSize()]);
  }

  @Nullable
  @Override
  public boolean[] getNullVector(String name)
  {
    final boolean[] defaultVector = new boolean[getCurrentVectorSize()];
    Arrays.fill(defaultVector, true);
    return nulls.getOrDefault(name, defaultVector);
  }

  @Override
  public int getMaxVectorSize()
  {
    return vectorSize;
  }

  @Override
  public int getCurrentVectorSize()
  {
    return vectorSize;
  }

  @Override
  public int getCurrentVectorId()
  {
    // never cache, this is just for tests anyway
    return id++;
  }
}
