/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.materializedview;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.StringUtils;
import io.druid.metadata.TestDerbyConnector;
import io.druid.segment.TestHelper;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.Map;
import java.util.Set;

public class MaterializedViewMetadataCoordinatorTest 
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private final ObjectMapper mapper = TestHelper.makeJsonMapper();
  
  private MaterializedViewMetadataCoordinator coordinator;
  private TestDerbyConnector derbyConnector;
  
  @Before
  public void setUp()
  {
    derbyConnector = derbyConnectorRule.getConnector();
    derbyConnector.createDataSourceTable();
    derbyConnector.createSegmentTable();
    coordinator = new MaterializedViewMetadataCoordinator(
        mapper,
        derbyConnectorRule.metadataTablesConfigSupplier(),
        derbyConnector
    );
  }
  
  @Test
  public void testInsertDataSourceMetadata() throws Exception
  {
    String dataSource = "wikiticker-derivatives";
    String baseDataSource = "wikiticker";
    Set<String> dims = Sets.newHashSet("dim1", "dim2");
    Set<String> metrics = Sets.newHashSet("metric1", "metric2", "metric3");
    DerivativeDataSourceMetadata metadata = new DerivativeDataSourceMetadata(baseDataSource, dims, metrics);
    coordinator.insertDataSourceMetadata(dataSource, metadata);
    Assert.assertEquals(
        metadata, 
        mapper.readValue(
            derbyConnector.lookup(
                derbyConnectorRule.metadataTablesConfigSupplier().get().getDataSourceTable(), 
                "dataSource", 
                "commit_metadata_payload", 
                dataSource
            ),
            DerivativeDataSourceMetadata.class
        )
    );
    
  }
  
  @Test
  public void testGetSegmentAndCreatedDate()
  {
    DataSegment segment = new DataSegment(
        "fooDataSource", 
        Intervals.of("2015-01-01T00Z/2015-01-02T00Z"), 
        "version", 
        ImmutableMap.<String, Object>of(), 
        ImmutableList.of("dim1"), 
        ImmutableList.of("m1"), 
        new NoneShardSpec(), 
        9, 
        100
    );
    String created_date = DateTimes.nowUtc().toString();
    Map<DataSegment, String> exceptedResult = Maps.newHashMap();
    exceptedResult.put(segment, created_date);
    derbyConnector.retryWithHandle(
        new HandleCallback<Boolean>() {
          @Override
          public Boolean withHandle(Handle handle) throws Exception 
          {
            handle.createStatement(
                StringUtils.format("INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, partitioned, version, used, payload) " +
                    "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
                    derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(), derbyConnector.getQuoteString()
                )
            )
                .bind("id", segment.getIdentifier()).bind("dataSource", segment.getDataSource())
                .bind("created_date", created_date)
                .bind("start", segment.getInterval().getStart().toString())
                .bind("end", segment.getInterval().getEnd().toString())
                .bind("partitioned", false)
                .bind("version", segment.getVersion())
                .bind("used", true)
                .bind("payload", mapper.writeValueAsBytes(segment))
                .execute();
            return true;
          }
        }
    );
    Map<DataSegment, String> segments = coordinator.getSegmentAndCreatedDate("fooDataSource", Intervals.of("2015-01-01T00Z/2015-01-02T00Z"));
    
    Assert.assertEquals(exceptedResult, segments);
    
  }
  
}
