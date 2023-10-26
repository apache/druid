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

package org.apache.druid.sql.calcite;

import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.operator.OffsetLimit;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.ScanOperatorFactory;

public class OperatorFactoryBuilders
{

  public static ScanOperatorFactoryBuilder scanOperatorFactoryBuilder()
  {
    return new ScanOperatorFactoryBuilder();
  }

  static class ScanOperatorFactoryBuilder
  {
    private OffsetLimit offsetLimit;
    private DimFilter filter;

    public OperatorFactory build()
    {
      return new ScanOperatorFactory(null, filter, offsetLimit, null, null, null);
    }

    public ScanOperatorFactoryBuilder setOffsetLimit(long offset, long limit)
    {
      offsetLimit = new OffsetLimit(offset, limit);
      return this;
    }

    public ScanOperatorFactoryBuilder setFilter(DimFilter filter)
    {
      this.filter = filter;
      return this;
    }
  }
}
