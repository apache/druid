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

package com.metamx.druid.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.db.DbConnector;
import com.metamx.druid.indexer.updater.DbUpdaterJobSpec;
import com.metamx.druid.jackson.DefaultObjectMapper;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.List;

/**
 */
public class DbUpdaterJob implements Jobby
{
  private static final Logger log = new Logger(DbUpdaterJob.class);

  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private final HadoopDruidIndexerConfig config;
  private final DbUpdaterJobSpec spec;
  private final DBI dbi;

  public DbUpdaterJob(
      HadoopDruidIndexerConfig config
  )
  {
    this.config = config;
    this.spec = (DbUpdaterJobSpec) config.getUpdaterJobSpec();
    this.dbi = new DbConnector(spec).getDBI();
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
                    "INSERT INTO %s (id, dataSource, created_date, start, end, partitioned, version, used, payload) "
                    + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
                    spec.getSegmentTable()
                )
            );
            for (final DataSegment segment : segments) {

              batch.add(
                  new ImmutableMap.Builder()
                      .put("id", segment.getIdentifier())
                      .put("dataSource", segment.getDataSource())
                      .put("created_date", new DateTime().toString())
                      .put("start", segment.getInterval().getStart().toString())
                      .put("end", segment.getInterval().getEnd().toString())
                      .put("partitioned", segment.getShardSpec().getPartitionNum())
                      .put("version", segment.getVersion())
                      .put("used", true)
                      .put("payload", jsonMapper.writeValueAsString(segment))
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
