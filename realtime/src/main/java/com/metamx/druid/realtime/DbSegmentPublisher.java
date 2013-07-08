package com.metamx.druid.realtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DbSegmentPublisher implements SegmentPublisher
{
  private static final Logger log = new Logger(DbSegmentPublisher.class);

  private final ObjectMapper jsonMapper;
  private final DbSegmentPublisherConfig config;
  private final DBI dbi;

  public DbSegmentPublisher(
      ObjectMapper jsonMapper,
      DbSegmentPublisherConfig config,
      DBI dbi
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.dbi = dbi;
  }

  public void publishSegment(final DataSegment segment) throws IOException
  {
    try {
      List<Map<String, Object>> exists = dbi.withHandle(
          new HandleCallback<List<Map<String, Object>>>()
          {
            @Override
            public List<Map<String, Object>> withHandle(Handle handle) throws Exception
            {
              return handle.createQuery(
                  String.format("SELECT id FROM %s WHERE id=:id", config.getSegmentTable())
              )
                           .bind("id", segment.getIdentifier())
                           .list();
            }
          }
      );

      if (!exists.isEmpty()) {
        log.info("Found [%s] in DB, not updating DB", segment.getIdentifier());
        return;
      }

      dbi.withHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              handle.createStatement(
                  String.format(
                      "INSERT INTO %s (id, dataSource, created_date, start, \"end\", partitioned, version, used, payload) "
                      + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
                      config.getSegmentTable()
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
    }
    catch (Exception e) {
      log.error(e, "Exception inserting into DB");
      throw new RuntimeException(e);
    }
  }
}
