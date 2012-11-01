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

import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.db.DbConnector;
import com.metamx.druid.indexer.updater.DbUpdaterJobSpec;
import com.metamx.druid.jackson.DefaultObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
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
  private volatile List<DataSegment> publishedSegments = null;

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

    List<DataSegment> newPublishedSegments = new LinkedList<DataSegment>();

    for (String propName : System.getProperties().stringPropertyNames()) {
      if (propName.startsWith("hadoop.")) {
        conf.set(propName.substring("hadoop.".length()), System.getProperty(propName));
      }
    }

    final Iterator<Bucket> buckets = config.getAllBuckets().iterator();
    Bucket bucket = buckets.next();
    int numRetried = 0;
    while (true) {
      try {
        final Path path = new Path(config.makeSegmentOutputPath(bucket), "descriptor.json");
        final DataSegment segment = jsonMapper.readValue(
            path.getFileSystem(conf).open(path),
            DataSegment.class
        );

        dbi.withHandle(
            new HandleCallback<Void>()
            {
              @Override
              public Void withHandle(Handle handle) throws Exception
              {
                handle.createStatement(
                    String.format(
                        "INSERT INTO %s (id, dataSource, created_date, start, end, partitioned, version, used, payload) VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
                        spec.getSegmentTable()
                    )
                )
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

        newPublishedSegments.add(segment);
        log.info("Published %s", segment.getIdentifier());
      }
      catch (Exception e) {
        if (numRetried < 5) {
          log.error(e, "Retrying[%d] after exception when loading segment metadata into db", numRetried);

          try {
            Thread.sleep(15 * 1000);
          }
          catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            return false;
          }

          ++numRetried;
          continue;
        }
        log.error(e, "Failing, retried too many times.");
        return false;
      }

      if (buckets.hasNext()) {
        bucket = buckets.next();
        numRetried = 0;
      } else {
        break;
      }
    }

    publishedSegments = newPublishedSegments;

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
      return Collections.unmodifiableList(publishedSegments);
    }
  }
}
