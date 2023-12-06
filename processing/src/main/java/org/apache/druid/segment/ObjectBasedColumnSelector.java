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

import org.apache.druid.error.DruidException;

/**
 * Object based column selector.
 *
 * This abstract class provides a {@link ColumnValueSelector} based on the methods of {@link BaseObjectColumnValueSelector}.
 */
public abstract class ObjectBasedColumnSelector<T> implements ColumnValueSelector<T>
{
  private static final String COLUMN_IS_NULL_ERROR_MESSAGE = "Invalid usage pattern: method returning primitive called - but the column is null";

  @Override
  public final float getFloat()
  {
    T value = getObject();
    if (value == null) {
      throw DruidException.defensive(COLUMN_IS_NULL_ERROR_MESSAGE);
    }
    return ((Number) value).floatValue();
  }

  @Override
  public final double getDouble()
  {
    T value = getObject();
    if (value == null) {
      throw DruidException.defensive(COLUMN_IS_NULL_ERROR_MESSAGE);
    }
    return ((Number) value).doubleValue();
  }

  @Override
  public final long getLong()
  {
    T value = getObject();
    if (value == null) {
      throw DruidException.defensive(COLUMN_IS_NULL_ERROR_MESSAGE);
    }
    return ((Number) value).longValue();
  }

  @Override
  public final boolean isNull()
  {
    T object = getObject();
    if (object == null) {
      return true;
    }
    if (object instanceof Number) {
      return false;
    }
    throw DruidException.defensive(
        "isNull() may only be called in case the underlying object is a Number but it was [%s]",
        object.getClass().getName()
    );
  }
}
