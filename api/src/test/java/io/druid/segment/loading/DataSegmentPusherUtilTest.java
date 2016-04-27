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

package io.druid.segment.loading;

import com.google.common.collect.ImmutableMap;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class DataSegmentPusherUtilTest
{
    @Test
    public void shouldNotHaveColonsInHdfsStorageDir() throws Exception {

        Interval interval = new Interval("2011-10-01/2011-10-02");
        ImmutableMap<String, Object> loadSpec = ImmutableMap.<String, Object>of("something", "or_other");

        DataSegment segment = new DataSegment(
                "something",
                interval,
                "brand:new:version",
                loadSpec,
                Arrays.asList("dim1", "dim2"),
                Arrays.asList("met1", "met2"),
                new NoneShardSpec(),
                null,
                1
        );

        String storageDir = DataSegmentPusherUtil.getHdfsStorageDir(segment);
        Assert.assertEquals("something/20111001T000000.000Z_20111002T000000.000Z/brand_new_version/0", storageDir);

    }
}
