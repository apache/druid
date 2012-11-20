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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.db.DbConnector;
import com.metamx.druid.indexer.updater.DbUpdaterJobSpec;
import com.metamx.druid.jackson.DefaultObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.io.IOException;
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

  // Keep track of published segment identifiers, in case a client is interested.
  private volatile ImmutableList<DataSegment> publishedSegments = null;

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
    final Configuration conf = new Configuration();

    ImmutableList.Builder<DataSegment> publishedSegmentsBuilder = ImmutableList.builder();

    for (String propName : System.getProperties().stringPropertyNames()) {
      if (propName.startsWith("hadoop.")) {
        conf.set(propName.substring("hadoop.".length()), System.getProperty(propName));
      }
    }

    final Path descriptorInfoDir = config.makeDescriptorInfoDir();

    try {
      FileSystem fs = descriptorInfoDir.getFileSystem(conf);

      for (FileStatus status : fs.listStatus(descriptorInfoDir)) {
        final DataSegment segment = jsonMapper.readValue(fs.open(status.getPath()), DataSegment.class);

        dbi.withHandle(
            new HandleCallback<Void>()
            {
              @Override
              public Void withHandle(Handle handle) throws Exception
              {
                handle.createStatement(String.format(
                        "INSERT INTO %s (id, dataSource, created_date, start, end, partitioned, version, used, payload) "
                        + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
                        spec.getSegmentTable()
                ))
                      .bind("id", segment.getIdentifier())
                      .bind("dataSource", segment.getDataSource())
                      .bind("created_date", new DateTime().toString())
                      .bind("start", segment.getInterval().getStart().toString())
                      .bind("end", segment.getInterval().getEnd().toString())
                      .bind("partitioned", segment.getShardSpec().getPartitionNum())
                      .bind("version", segment.getVersion())
                      .bind("used", true)
                      .bind("payload", jsonMapper.writeValueAsString(segment))
                      .execute();

                return null;
              }
            }
        );

        publishedSegmentsBuilder.add(segment);
        log.info("Published %s", segment.getIdentifier());
      }
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    publishedSegments = publishedSegmentsBuilder.build();

    return true;
  }

  /**
   * Returns a list of segment identifiers published by the most recent call to run().
   * Throws an IllegalStateException if run() has never been called.
   */
  public List<DataSegment> getPublishedSegments()
  {
    if (publishedSegments == null) {
      log.error("getPublishedSegments called before run!");
      throw new IllegalStateException("DbUpdaterJob has not run yet");
    } else {
      return publishedSegments;
    }
  }
}
