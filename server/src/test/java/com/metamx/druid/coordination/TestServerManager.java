/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.coordination;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.MapUtils;
import com.metamx.druid.Query;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.Segment;
import com.metamx.druid.index.v1.IndexIO;
import com.metamx.druid.loading.SegmentLoader;
import com.metamx.druid.loading.SegmentLoadingException;
import com.metamx.druid.metrics.NoopServiceEmitter;
import com.metamx.druid.query.QueryRunnerFactory;
import com.metamx.druid.query.QueryRunnerFactoryConglomerate;
import com.metamx.druid.shard.NoneShardSpec;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Arrays;

/**
 */
public class TestServerManager extends ServerManager
{
  public TestServerManager(
      final QueryRunnerFactory factory
  )
  {
    super(
        new SegmentLoader()
        {
          @Override
          public boolean isSegmentLoaded(DataSegment segment) throws SegmentLoadingException
          {
            return false;
          }

          @Override
          public Segment getSegment(final DataSegment segment)
          {
            return new SegmentForTesting(
                MapUtils.getString(segment.getLoadSpec(), "version"),
                (Interval) segment.getLoadSpec().get("interval")
            );
          }

          @Override
          public void cleanup(DataSegment segment) throws SegmentLoadingException
          {

          }
        },
        new QueryRunnerFactoryConglomerate()
        {
          @Override
          public <T, QueryType extends Query<T>> QueryRunnerFactory<T, QueryType> findFactory(QueryType query)
          {
            return (QueryRunnerFactory) factory;
          }
        },
        new NoopServiceEmitter(),
        MoreExecutors.sameThreadExecutor()
    );
  }

  public void loadQueryable(String dataSource, String version, Interval interval) throws IOException
  {
    try {
      super.loadSegment(
          new DataSegment(
              dataSource,
              interval,
              version,
              ImmutableMap.<String, Object>of("version", version, "interval", interval),
              Arrays.asList("dim1", "dim2", "dim3"),
              Arrays.asList("metric1", "metric2"),
              new NoneShardSpec(),
              IndexIO.CURRENT_VERSION_ID,
              123l
          )
      );
    }
    catch (SegmentLoadingException e) {
      throw new RuntimeException(e);
    }
  }

  public void dropQueryable(String dataSource, String version, Interval interval)
  {
    try {
      super.dropSegment(
          new DataSegment(
              dataSource,
              interval,
              version,
              ImmutableMap.<String, Object>of("version", version, "interval", interval),
              Arrays.asList("dim1", "dim2", "dim3"),
              Arrays.asList("metric1", "metric2"),
              new NoneShardSpec(),
              IndexIO.CURRENT_VERSION_ID,
              123l
          )
      );
    }
    catch (SegmentLoadingException e) {
      throw new RuntimeException(e);
    }
  }
}
