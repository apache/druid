/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexer;

import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.BaseProgressIndicator;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexSpec;
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
      return IndexMerger.persist(index, interval, file, config.getIndexSpec(), progressIndicator);
    }

    @Override
    protected File mergeQueryableIndex(
        List<QueryableIndex> indexes,
        AggregatorFactory[] aggs,
        File file,
        ProgressIndicator progressIndicator
    ) throws IOException
    {
      return IndexMerger.mergeQueryableIndex(indexes, aggs, file, config.getIndexSpec(), progressIndicator);
    }
  }
}
