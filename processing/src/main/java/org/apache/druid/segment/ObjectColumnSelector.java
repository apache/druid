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
 * Restricts selector usage to only allow {@link #getObject()}.
 *
 * This class is convenient for implementation of "object-sourcing"
 * {@link ColumnValueSelector}s.
 *
 * This class should appear ONLY in "extends" clause or anonymous class
 * creation, but NOT in "user" code, where {@link BaseObjectColumnValueSelector}
 * must be used instead.
 */
public abstract class ObjectColumnSelector<T> implements ColumnValueSelector<T>
{
  private static final String EXCEPTION_MESSAGE = "This class has restricted API; use getObject() instead";

  @Override
  public final float getFloat()
  {
    throw DruidException.defensive(EXCEPTION_MESSAGE);
  }

  @Override
  public final double getDouble()
  {
    throw DruidException.defensive(EXCEPTION_MESSAGE);
  }

  @Override
  public final long getLong()
  {
    throw DruidException.defensive(EXCEPTION_MESSAGE);
  }

  @Override
  public final boolean isNull()
  {
    throw DruidException.defensive(EXCEPTION_MESSAGE);
  }
}
