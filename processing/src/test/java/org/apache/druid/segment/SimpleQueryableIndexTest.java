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
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.Indexed;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;


public class SimpleQueryableIndexTest
{
  @Test
  public void testTombstoneDefaultInterface()
  {
    QueryableIndex qi = new QueryableIndex()
    {
      @Override
      public Interval getDataInterval()
      {
        return null;
      }

      @Override
      public int getNumRows()
      {
        return 0;
      }

      @Override
      public Indexed<String> getAvailableDimensions()
      {
        return null;
      }

      @Override
      public BitmapFactory getBitmapFactoryForDimensions()
      {
        return null;
      }

      @Nullable
      @Override
      public Metadata getMetadata()
      {
        return null;
      }

      @Override
      public Map<String, DimensionHandler> getDimensionHandlers()
      {
        return null;
      }

      @Override
      public void close()
      {

      }

      @Override
      public List<String> getColumnNames()
      {
        return null;
      }

      @Nullable
      @Override
      public ColumnHolder getColumnHolder(String columnName)
      {
        return null;
      }
    };

    Assert.assertFalse(qi.isFromTombstone());
  }

}
