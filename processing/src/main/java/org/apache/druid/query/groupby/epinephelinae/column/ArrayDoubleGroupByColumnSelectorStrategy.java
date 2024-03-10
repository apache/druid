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

package org.apache.druid.query.groupby.epinephelinae.column;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;

import java.util.Arrays;
import java.util.List;

public class ArrayDoubleGroupByColumnSelectorStrategy extends ArrayNumericGroupByColumnSelectorStrategy
{
  public ArrayDoubleGroupByColumnSelectorStrategy()
  {
    super(Double.BYTES, ColumnType.DOUBLE_ARRAY);
  }

  @Override
  protected int computeDictionaryId(ColumnValueSelector selector)
  {
    Object object = selector.getObject();
    if (object == null) {
      return GROUP_BY_MISSING_VALUE;
    } else if (object instanceof Double) {
      return addToIndexedDictionary(new Object[]{object});
    } else if (object instanceof List) {
      return addToIndexedDictionary(((List) object).toArray());
    } else if (object instanceof Double[]) {
      // Defensive check, since we don't usually expect to encounter Double[] objects from selectors
      return addToIndexedDictionary(Arrays.stream((Double[]) object).toArray());
    } else if (object instanceof Object[]) {
      return addToIndexedDictionary((Object[]) object);
    } else {
      throw new ISE("Found unexpected object type [%s] in %s array.", object.getClass().getName(), ValueType.DOUBLE);
    }
  }
}


