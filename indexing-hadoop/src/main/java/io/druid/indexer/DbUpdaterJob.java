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

import com.google.common.collect.ImmutableMap;
import com.metamx.common.logger.Logger;
import io.druid.db.DbConnector;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.List;

/**
 */
public class DbUpdaterJob implements Jobby
{
  private static final Logger log = new Logger(DbUpdaterJob.class);

  private final HadoopDruidIndexerConfig config;
  private final IDBI dbi;
  private final DbConnector dbConnector;

  public DbUpdaterJob(
      HadoopDruidIndexerConfig config
  )
  {
    this.config = config;
    this.dbConnector = new DbConnector(config.getSpec().getIOConfig().getMetadataUpdateSpec(), null);
    this.dbi = this.dbConnector.getDBI();
  }

  @Override
  public boolean run()
  {
    final List<DataSegment> segments = IndexGeneratorJob.getPublishedSegments(config);

    dbi.withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            final PreparedBatch batch = handle.prepareBatch(
                String.format(
                    dbConnector.isPostgreSQL() ?
                      "INSERT INTO %s (id, dataSource, created_date, start, \"end\", partitioned, version, used, payload) "
                      + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)" :
                      "INSERT INTO %s (id, dataSource, created_date, start, end, partitioned, version, used, payload) "
                      + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
                    config.getSpec().getIOConfig().getMetadataUpdateSpec().getSegmentTable()
                )
            );
            for (final DataSegment segment : segments) {

              batch.add(
                  new ImmutableMap.Builder<String, Object>()
                      .put("id", segment.getIdentifier())
                      .put("dataSource", segment.getDataSource())
                      .put("created_date", new DateTime().toString())
                      .put("start", segment.getInterval().getStart().toString())
                      .put("end", segment.getInterval().getEnd().toString())
                      .put("partitioned", (segment.getShardSpec() instanceof NoneShardSpec) ? 0 : 1)
                      .put("version", segment.getVersion())
                      .put("used", true)
                      .put("payload", HadoopDruidIndexerConfig.jsonMapper.writeValueAsBytes(segment))
                      .build()
              );

              log.info("Published %s", segment.getIdentifier());

            }
            batch.execute();

            return null;
          }
        }
    );

    return true;
  }
}
