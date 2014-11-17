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

package io.druid.indexer;

import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.BaseProgressIndicator;
import io.druid.segment.IndexMerger;
import io.druid.segment.ProgressIndicator;
import io.druid.segment.QueryableIndex;
import io.druid.segment.incremental.IncrementalIndex;
import org.apache.hadoop.mapreduce.Job;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 */
public class LegacyIndexGeneratorJob extends IndexGeneratorJob
{
  public LegacyIndexGeneratorJob(
      HadoopDruidIndexerConfig config
  )
  {
    super(config);
  }

  @Override
  protected void setReducerClass(Job job)
  {
    job.setReducerClass(LegacyIndexGeneratorReducer.class);
  }

  public static class LegacyIndexGeneratorReducer extends IndexGeneratorJob.IndexGeneratorReducer
  {
    @Override
    protected ProgressIndicator makeProgressIndicator(final Context context)
    {
      return new BaseProgressIndicator()
      {
        @Override
        public void progress()
        {
          context.progress();
        }
      };
    }

    @Override
    protected File persist(
        IncrementalIndex index, Interval interval, File file, ProgressIndicator progressIndicator
    ) throws IOException
    {
      return IndexMerger.persist(index, interval, file, progressIndicator);
    }

    @Override
    protected File mergeQueryableIndex(
        List<QueryableIndex> indexes,
        AggregatorFactory[] aggs,
        File file,
        ProgressIndicator progressIndicator
    ) throws IOException
    {
      return IndexMerger.mergeQueryableIndex(indexes, aggs, file, progressIndicator);
    }
  }
}
