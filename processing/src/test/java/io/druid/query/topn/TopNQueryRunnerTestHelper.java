/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.topn;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.query.Result;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

/**
 */
public class TopNQueryRunnerTestHelper
{
  public static Result<TopNResultValue> createExpectedRows(String date, String[] columnNames, Iterable<Object[]> values)
  {
    List<Map> expected = Lists.newArrayList();
    for (Object[] value : values) {
      Preconditions.checkArgument(value.length == columnNames.length);
      Map<String, Object> theVals = Maps.newHashMapWithExpectedSize(value.length);
      for (int i = 0; i < columnNames.length; i++) {
        theVals.put(columnNames[i], value[i]);
      }
      expected.add(theVals);
    }
    return new Result<TopNResultValue>(new DateTime(date), new TopNResultValue(expected));
  }
}
