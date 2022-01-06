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

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.ColumnValueSelector;

import java.util.Arrays;
import java.util.List;

public class ListDoubleGroupByColumnSelectorStrategy extends ListGroupByColumnSelectorStrategy<Double>
{
  @Override
  public Object getOnlyValue(ColumnValueSelector selector)
  {
    Object object = selector.getObject();
    if (object == null) {
      return GROUP_BY_MISSING_VALUE;
    } else if (object instanceof Double) {
      return addToIndexedDictionary(ImmutableList.of((Double) object));
    } else if (object instanceof List) {
      return addToIndexedDictionary((List<Double>) object);
    } else if (object instanceof Double[]) {
      return addToIndexedDictionary(Arrays.asList((Double[]) object));
    } else {
      throw new ISE("Found unknowm type %s in ColumnValueSelector.", object.getClass().toString());
    }
  }
}


