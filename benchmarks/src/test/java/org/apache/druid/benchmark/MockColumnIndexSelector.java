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

package org.apache.druid.benchmark;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;

import javax.annotation.Nullable;

public class MockColumnIndexSelector implements ColumnIndexSelector
{
  private final BitmapFactory bitmapFactory;
  private final ColumnIndexSupplier indexSupplier;

  public MockColumnIndexSelector(
      BitmapFactory bitmapFactory,
      ColumnIndexSupplier indexSupplier
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.indexSupplier = indexSupplier;
  }

  @Override
  public int getNumRows()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public BitmapFactory getBitmapFactory()
  {
    return bitmapFactory;
  }

  @Override
  public ColumnIndexSupplier getIndexSupplier(String column)
  {
    return indexSupplier;
  }

  @Nullable
  @Override
  public ColumnHolder getColumnHolder(String columnName)
  {
    return null;
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return null;
  }

}
