package com.metamx.druid.loading;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.db.DbConnectorConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.joda.time.Interval;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 */
public class S3SegmentKiller implements SegmentKiller
{
  private static final Logger log = new Logger(S3SegmentKiller.class);

  private final RestS3Service s3Client;
  private final DBI dbi;
  private final DbConnectorConfig config;
  private final ObjectMapper jsonMapper;

  @Inject
  public S3SegmentKiller(
      RestS3Service s3Client,
      DBI dbi,
      DbConnectorConfig config,
      ObjectMapper jsonMapper
  )
  {
    this.s3Client = s3Client;
    this.dbi = dbi;
    this.config = config;
    this.jsonMapper = jsonMapper;
  }


  @Override
  public List<DataSegment> kill(final String datasource, final Interval interval) throws ServiceException
  {
    List<DataSegment> matchingSegments = dbi.withHandle(
        new HandleCallback<List<DataSegment>>()
        {
          @Override
          public List<DataSegment> withHandle(Handle handle) throws Exception
          {
            return handle.createQuery(
                String.format(
                    "SELECT payload FROM %s WHERE dataSource = :dataSource and start >= :start and end <= :end and used = 0",
                    config.getSegmentTable()
                )
            )
                         .bind("dataSource", datasource)
                         .bind("start", interval.getStart().toString())
                         .bind("end", interval.getEnd().toString())
                         .fold(
                             Lists.<DataSegment>newArrayList(),
                             new Folder3<List<DataSegment>, Map<String, Object>>()
                             {
                               @Override
                               public List<DataSegment> fold(
                                   List<DataSegment> accumulator,
                                   Map<String, Object> stringObjectMap,
                                   FoldController foldController,
                                   StatementContext statementContext
                               ) throws SQLException
                               {
                                 try {
                                   DataSegment segment = jsonMapper.readValue(
                                       (String) stringObjectMap.get("payload"),
                                       DataSegment.class
                                   );

                                   accumulator.add(segment);

                                   return accumulator;
                                 }
                                 catch (Exception e) {
                                   throw Throwables.propagate(e);
                                 }
                               }
                             }
                         );
          }
        }
    );

    log.info("Found %,d segments for %s for interval %s.", matchingSegments.size(), datasource, interval);
    for (final DataSegment segment : matchingSegments) {
      // Remove from S3
      Map<String, Object> loadSpec = segment.getLoadSpec();
      String s3Bucket = MapUtils.getString(loadSpec, "bucket");
      String s3Path = MapUtils.getString(loadSpec, "key");
      String s3DescriptorPath = s3Path.substring(0, s3Path.lastIndexOf("/")) + "/descriptor.json";

      if (s3Client.isObjectInBucket(s3Bucket, s3Path)) {
        log.info("Removing index file[s3://%s/%s] from s3!", s3Bucket, s3Path);
        s3Client.deleteObject(s3Bucket, s3Path);
      }
      if (s3Client.isObjectInBucket(s3Bucket, s3DescriptorPath)) {
        log.info("Removing descriptor file[s3://%s/%s] from s3!", s3Bucket, s3DescriptorPath);
        s3Client.deleteObject(s3Bucket, s3DescriptorPath);
      }
    }

    return matchingSegments;
  }
}
