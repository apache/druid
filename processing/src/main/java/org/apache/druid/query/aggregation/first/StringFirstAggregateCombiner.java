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

package org.apache.druid.query.aggregation.first;

import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public class StringFirstAggregateCombiner extends ObjectAggregateCombiner<String>
{
  private String firstString;
  private boolean isReset = false;

  @Override
  public void reset(ColumnValueSelector selector)
  {
    firstString = (String) selector.getObject();
    isReset = true;
  }

  @Override
  public void fold(ColumnValueSelector selector)
  {
    if (!isReset) {
      firstString = (String) selector.getObject();
      isReset = true;
    }
  }

  @Nullable
  @Override
  public String getObject()
  {
    return firstString;
  }

  @Override
  public Class<String> classOfObject()
  {
    return String.class;
  }
}
