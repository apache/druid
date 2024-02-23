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

package org.apache.druid.query.rowsandcols;

import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.rowsandcols.column.Column;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

public class TestRowsAndColumns implements RowsAndColumns
{
  @SuppressWarnings("rawtypes")
  private final Map<Class, Supplier> asExpectations = new LinkedHashMap<>();

  @Override
  public Collection<String> getColumnNames()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int numRows()
  {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public Column findColumn(String name)
  {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    final Supplier supplier = asExpectations.get(clazz);
    if (supplier == null) {
      throw new UOE("Cannot become class [%s], maybe need to call withAsImpl?", clazz);
    } else {
      return (T) supplier.get();
    }
  }

  public <T> TestRowsAndColumns withAsImpl(Class<T> clazz, Supplier<T> supp)
  {
    asExpectations.put(clazz, supp);
    return this;
  }
}
