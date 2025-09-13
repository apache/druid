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

package org.apache.druid.segment;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.column.BaseColumnHolder;
import org.apache.druid.segment.data.Indexed;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * A no-op implementation of QueryableIndex. Throws UnsupportedOperationException for all methods, except {@link #close()}.
 */
public class NoopQueryableIndex implements QueryableIndex
{
  @Override
  public Interval getDataInterval()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNumRows()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public BitmapFactory getBitmapFactoryForDimensions()
  {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public Metadata getMetadata()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, DimensionHandler> getDimensionHandlers()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<OrderBy> getOrdering()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close()
  {

  }

  @Override
  public List<String> getColumnNames()
  {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public BaseColumnHolder getColumnHolder(String columnName)
  {
    throw new UnsupportedOperationException();
  }
}
