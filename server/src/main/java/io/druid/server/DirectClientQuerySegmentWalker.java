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

package io.druid.server;

import io.druid.client.DirectDruidClient;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

/**
 */
public class DirectClientQuerySegmentWalker implements QuerySegmentWalker
{
  private final QueryToolChestWarehouse warehouse;
  private final DirectDruidClient baseClient;

  public DirectClientQuerySegmentWalker(
      QueryToolChestWarehouse warehouse,
      DirectDruidClient baseClient
  )
  {
    this.warehouse = warehouse;
    this.baseClient = baseClient;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    return makeRunner(query);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    return makeRunner(query);
  }

  private <T> FinalizeResultsQueryRunner<T> makeRunner(final Query<T> query)
  {
    return new FinalizeResultsQueryRunner<T>(baseClient, warehouse.getToolChest(query));
  }
}
