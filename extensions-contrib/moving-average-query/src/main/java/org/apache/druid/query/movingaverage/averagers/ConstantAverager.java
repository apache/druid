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

package org.apache.druid.query.movingaverage.averagers;

import org.apache.druid.query.aggregation.AggregatorFactory;

import java.util.Map;

/**
 * The constant averager.Created soley for incremental development and wiring things up.
 */
public class ConstantAverager implements Averager<Float>
{

  private String name;
  private float retval;

  /**
   * @param n
   * @param name
   * @param retval
   */
  public ConstantAverager(int n, String name, float retval)
  {
    this.name = name;
    this.retval = retval;
  }

  /* (non-Javadoc)
   * @see Averager#getResult()
   */
  @Override
  public Float getResult()
  {
    return retval;
  }

  /* (non-Javadoc)
   * @see Averager#getName()
   */
  @Override
  public String getName()
  {
    return name;
  }

  /* (non-Javadoc)
   * @see Averager#addElement(java.util.Map, java.util.Map)
   */
  @Override
  public void addElement(Map<String, Object> e, Map<String, AggregatorFactory> a)
  {
    // since we return a constant, no need to read from the event
  }

  /* (non-Javadoc)
   * @see Averager#skip()
   */
  @Override
  public void skip()
  {
  }

}
