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

package org.apache.druid.query.materializedview;

import com.google.common.base.Preconditions;

import java.util.Objects;
import java.util.Set;

public class DerivativeDataSource implements Comparable<DerivativeDataSource>
{
  private final String name;
  private final String baseDataSource;
  private final Set<String> columns;
  private final long avgSizeBasedGranularity;

  public DerivativeDataSource(String name, String baseDataSource, Set<String> columns, long size)
  {
    this.name = Preconditions.checkNotNull(name, "name");
    this.baseDataSource = Preconditions.checkNotNull(baseDataSource, "baseDataSource");
    this.columns = Preconditions.checkNotNull(columns, "columns");
    this.avgSizeBasedGranularity = size;
  }

  public String getName()
  {
    return name;
  }

  public String getBaseDataSource()
  {
    return baseDataSource;
  }
  
  public Set<String> getColumns()
  {
    return columns;
  }

  public long getAvgSizeBasedGranularity()
  {
    return avgSizeBasedGranularity;
  }

  @Override
  public int compareTo(DerivativeDataSource o)
  {
    if (this.avgSizeBasedGranularity > o.getAvgSizeBasedGranularity()) {
      return 1;
    } else if (this.avgSizeBasedGranularity == o.getAvgSizeBasedGranularity()) {
      return 0;
    } else {
      return -1;
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null) {
      return false;
    }
    if (!(o instanceof DerivativeDataSource)) {
      return false;
    }
    DerivativeDataSource that = (DerivativeDataSource) o;
    return name.equals(that.getName()) 
        && baseDataSource.equals(that.getBaseDataSource())
        && columns.equals(that.getColumns());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, baseDataSource, columns);
  }
}
