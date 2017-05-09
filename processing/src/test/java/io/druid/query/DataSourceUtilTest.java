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

package io.druid.query;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class DataSourceUtilTest
{

  @Test
  public void testGetMetricNameFromDataSource()
  {
    final DataSource source = new TableDataSource("table");
    Assert.assertEquals("table", DataSourceUtil.getMetricName(source));

    final DataSource unionSource = new UnionDataSource(
        Lists.newArrayList(
            new TableDataSource("src1"),
            new TableDataSource("src2"),
            new TableDataSource("src3"),
            new TableDataSource("src4")
        )
    );
    Assert.assertEquals("[src1, src2, src3, src4]", DataSourceUtil.getMetricName(unionSource));
  }
}
