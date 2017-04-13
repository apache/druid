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
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

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

  @Test
  public void testGetMetricNameFromDataSourceWithSegmentSpec()
  {
    final List<DataSourceWithSegmentSpec> specs = Lists.newArrayList(
        new DataSourceWithSegmentSpec(
            new TableDataSource("src1"),
            new MultipleIntervalSegmentSpec(
                Lists.newArrayList(
                    new Interval(0, 100),
                    new Interval(200, 300),
                    new Interval(400, 500)
                )
            )
        ),
        new DataSourceWithSegmentSpec(
            new TableDataSource("src2"),
            new MultipleIntervalSegmentSpec(
                Lists.newArrayList(
                    new Interval(0, 10),
                    new Interval(20, 30),
                    new Interval(40, 50)
                )
            )
        )
    );
    Assert.assertEquals(
        "[src1=[1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.100Z, "
        + "1970-01-01T00:00:00.200Z/1970-01-01T00:00:00.300Z, "
        + "1970-01-01T00:00:00.400Z/1970-01-01T00:00:00.500Z], "
        + "src2=[1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.010Z, "
        + "1970-01-01T00:00:00.020Z/1970-01-01T00:00:00.030Z, "
        + "1970-01-01T00:00:00.040Z/1970-01-01T00:00:00.050Z]]",
        DataSourceUtil.getMetricName(specs)
    );
  }
}
