/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.loading;

import com.google.common.collect.ImmutableMap;
import io.druid.segment.IndexIO;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class DataSegmentPusherUtilTest {
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
                IndexIO.CURRENT_VERSION_ID,
                1
        );

        String storageDir = DataSegmentPusherUtil.getHdfsStorageDir(segment);
        Assert.assertEquals("something/20111001T000000.000Z_20111002T000000.000Z/brand_new_version/0", storageDir);

    }
}
