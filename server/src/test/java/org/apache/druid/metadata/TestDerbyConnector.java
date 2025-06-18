/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.metadata.storage.derby.DerbyConnector;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.rules.ExternalResource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

public class TestDerbyConnector extends DerbyConnector
{
  private final String jdbcUri;
  private final MetadataStorageTablesConfig dbTables;

  public TestDerbyConnector(
      MetadataStorageConnectorConfig connectorConfig,
      MetadataStorageTablesConfig tablesConfig,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
  )
  {
    this(
        connectorConfig,
        tablesConfig,
        "jdbc:derby:memory:druidTest" + dbSafeUUID(),
        centralizedDatasourceSchemaConfig
    );
  }

  public TestDerbyConnector(
      MetadataStorageConnectorConfig connectorConfig,
      MetadataStorageTablesConfig tablesConfig
  )
  {
    this(
        connectorConfig,
        tablesConfig,
        "jdbc:derby:memory:druidTest" + dbSafeUUID(),
        CentralizedDatasourceSchemaConfig.create()
    );
  }

  protected TestDerbyConnector(
      MetadataStorageConnectorConfig connectorConfig,
      MetadataStorageTablesConfig tablesConfig,
      String jdbcUri,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
  )
  {
    super(
        new NoopMetadataStorageProvider().get(),
        Suppliers.ofInstance(connectorConfig),
        Suppliers.ofInstance(tablesConfig),
        new DBI(jdbcUri + ";create=true"),
        centralizedDatasourceSchemaConfig
    );
    this.jdbcUri = jdbcUri;
    this.dbTables = tablesConfig;
  }

  public TestDerbyConnector()
  {
    this(
        new MetadataStorageConnectorConfig(),
        MetadataStorageTablesConfig.fromBase("druidTest" + dbSafeUUID()),
        CentralizedDatasourceSchemaConfig.create()
    );
  }

  public MetadataStorageTablesConfig getMetadataTablesConfig()
  {
    return this.dbTables;
  }

  public void tearDown()
  {
    try {
      new DBI(jdbcUri + ";drop=true").open().close();
    }
    catch (UnableToObtainConnectionException e) {
      SQLException cause = (SQLException) e.getCause();
      // error code "08006" indicates proper shutdown
      Assert.assertEquals(
          StringUtils.format("Derby not shutdown: [%s]", cause.toString()),
          "08006",
          cause.getSQLState()
      );
    }
  }

  public static String dbSafeUUID()
  {
    return StringUtils.removeChar(UUID.randomUUID().toString(), '-');
  }

  public String getJdbcUri()
  {
    return jdbcUri;
  }

  public void createDatabase()
  {
    this.getDBI().open().close();
  }

  public static class DerbyConnectorRule extends ExternalResource
  {
    private TestDerbyConnector connector;
    private final MetadataStorageTablesConfig tablesConfig;
    private final MetadataStorageConnectorConfig connectorConfig;
    private final CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig;

    public DerbyConnectorRule()
    {
      this("druidTest" + dbSafeUUID());
    }

    public DerbyConnectorRule(CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig)
    {
      this(
          MetadataStorageTablesConfig.fromBase("druidTest" + dbSafeUUID()),
          centralizedDatasourceSchemaConfig
      );
    }

    private DerbyConnectorRule(
        final String defaultBase
    )
    {
      this(
          MetadataStorageTablesConfig.fromBase(defaultBase),
          CentralizedDatasourceSchemaConfig.create()
      );
    }

    public DerbyConnectorRule(
        MetadataStorageTablesConfig tablesConfig,
        CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
    )
    {
      this.tablesConfig = tablesConfig;
      this.connectorConfig = new MetadataStorageConnectorConfig()
      {
        @Override
        public String getConnectURI()
        {
          return connector.getJdbcUri();
        }
      };
      this.centralizedDatasourceSchemaConfig = centralizedDatasourceSchemaConfig;
    }

    @Override
    public void before()
    {
      connector = new TestDerbyConnector(
          connectorConfig,
          tablesConfig,
          centralizedDatasourceSchemaConfig
      );
      connector.createDatabase(); // create db
    }

    @Override
    public void after()
    {
      connector.tearDown();
    }

    public TestDerbyConnector getConnector()
    {
      return connector;
    }

    public MetadataStorageConnectorConfig getMetadataConnectorConfig()
    {
      return connectorConfig;
    }

    public Supplier<MetadataStorageTablesConfig> metadataTablesConfigSupplier()
    {
      return Suppliers.ofInstance(tablesConfig);
    }

    public SegmentsTable segments()
    {
      return new SegmentsTable(this);
    }

    public PendingSegmentsTable pendingSegments()
    {
      return new PendingSegmentsTable(this);
    }
  }

  /**
   * Wrapper class for interacting with the pending segments table.
   */
  public static class PendingSegmentsTable
  {
    private final DerbyConnectorRule rule;

    private PendingSegmentsTable(DerbyConnectorRule rule)
    {
      this.rule = rule;
    }

    public int insert(
        List<PendingSegmentRecord> records,
        boolean skipSegmentLineageCheck,
        ObjectMapper objectMapper
    )
    {
      final TestDerbyConnector connector = rule.getConnector();
      final String sql = StringUtils.format(
          "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, sequence_name, sequence_prev_id, "
          + "sequence_name_prev_id_sha1, payload, task_allocator_id, upgraded_from_segment_id) "
          + "VALUES (:id, :dataSource, :created_date, :start, :end, :sequence_name, :sequence_prev_id, "
          + ":sequence_name_prev_id_sha1, :payload, :task_allocator_id, :upgraded_from_segment_id)",
          rule.metadataTablesConfigSupplier().get().getPendingSegmentsTable(),
          connector.getQuoteString()
      );

      return connector.retryWithHandle(handle -> {
        final PreparedBatch insertBatch = handle.prepareBatch(sql);

        final Set<SegmentIdWithShardSpec> processedSegmentIds = new HashSet<>();
        for (PendingSegmentRecord pendingSegment : records) {
          final SegmentIdWithShardSpec segmentId = pendingSegment.getId();
          if (processedSegmentIds.contains(segmentId)) {
            continue;
          }
          final Interval interval = segmentId.getInterval();

          insertBatch.add()
                     .bind("id", segmentId.toString())
                     .bind("dataSource", segmentId.getDataSource())
                     .bind("created_date", pendingSegment.getCreatedDate().toString())
                     .bind("start", interval.getStart().toString())
                     .bind("end", interval.getEnd().toString())
                     .bind("sequence_name", pendingSegment.getSequenceName())
                     .bind("sequence_prev_id", pendingSegment.getSequencePrevId())
                     .bind(
                         "sequence_name_prev_id_sha1",
                         pendingSegment.computeSequenceNamePrevIdSha1(skipSegmentLineageCheck)
                     )
                     .bind("payload", JacksonUtils.toBytes(objectMapper, segmentId))
                     .bind("task_allocator_id", pendingSegment.getTaskAllocatorId())
                     .bind("upgraded_from_segment_id", pendingSegment.getUpgradedFromSegmentId());

          processedSegmentIds.add(segmentId);
        }
        int[] updated = insertBatch.execute();
        return Arrays.stream(updated).sum();
      });
    }
  }

  /**
   * A wrapper class for updating the segments table.
   */
  public static class SegmentsTable
  {
    private final DerbyConnectorRule rule;

    private SegmentsTable(DerbyConnectorRule rule)
    {
      this.rule = rule;
    }

    /**
     * Updates the segments table with the supplied SQL query format and arguments.
     *
     * @param sqlFormat the SQL query format with %s placeholder for the table name and ? for each query {@code args}
     * @param args      the arguments to be substituted into the SQL query
     * @return the number of rows affected by the update operation
     */
    public int update(String sqlFormat, Object... args)
    {
      return this.rule.getConnector().retryWithHandle(
          handle -> handle.update(
              StringUtils.format(sqlFormat, getTableName()),
              args
          )
      );
    }

    public int updateUsedStatusLastUpdated(String segmentId, DateTime lastUpdatedTime)
    {
      return update(
          "UPDATE %1$s SET USED_STATUS_LAST_UPDATED = ? WHERE ID = ?",
          lastUpdatedTime.toString(),
          segmentId
      );
    }

    public String getTableName()
    {
      return this.rule.metadataTablesConfigSupplier()
                      .get()
                      .getSegmentsTable()
                      .toUpperCase(Locale.ENGLISH);
    }
  }

  public static class DerbyConnectorRule5 extends DerbyConnectorRule implements BeforeAllCallback, AfterAllCallback
  {

    @Override
    public void beforeAll(ExtensionContext context)
    {
      before();
    }

    @Override
    public void afterAll(ExtensionContext context)
    {
      after();
    }
  }
}
