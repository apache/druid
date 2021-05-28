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
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.overlord.supervisor.NoopSupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.VersionedSupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoScaler;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SQLMetadataSupervisorManagerTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig;
  private SQLMetadataSupervisorManager supervisorManager;

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  @BeforeClass
  public static void setupStatic()
  {
    MAPPER.registerSubtypes(TestSupervisorSpec.class);
  }

  @After
  public void cleanup()
  {
    connector.getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle)
          {
            handle.createStatement(StringUtils.format("DROP TABLE %s", tablesConfig.getSupervisorTable()))
                  .execute();
            return null;
          }
        }
    );
  }

  @Before
  public void setUp()
  {
    connector = derbyConnectorRule.getConnector();
    tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    connector.createSupervisorsTable();

    supervisorManager = new SQLMetadataSupervisorManager(MAPPER, connector, Suppliers.ofInstance(tablesConfig));
  }

  @Test
  public void testRemoveTerminatedSupervisorsOlderThanSupervisorActiveShouldNotBeDeleted()
  {
    final String supervisor1 = "test-supervisor-1";
    final Map<String, String> data1rev1 = ImmutableMap.of("key1-1", "value1-1-1", "key1-2", "value1-2-1");
    Assert.assertTrue(supervisorManager.getAll().isEmpty());
    supervisorManager.insert(supervisor1, new TestSupervisorSpec(supervisor1, data1rev1));
    // Test that supervisor was inserted
    Map<String, List<VersionedSupervisorSpec>> supervisorSpecs = supervisorManager.getAll();
    Assert.assertEquals(1, supervisorSpecs.size());
    Map<String, SupervisorSpec> latestSpecs = supervisorManager.getLatest();
    Assert.assertEquals(1, latestSpecs.size());
    // Try delete. Supervisor should not be deleted as it is still active
    int deleteCount = supervisorManager.removeTerminatedSupervisorsOlderThan(System.currentTimeMillis());
    // Test that supervisor was not deleted
    Assert.assertEquals(0, deleteCount);
    supervisorSpecs = supervisorManager.getAll();
    Assert.assertEquals(1, supervisorSpecs.size());
    latestSpecs = supervisorManager.getLatest();
    Assert.assertEquals(1, latestSpecs.size());
  }

  @Test
  public void testRemoveTerminatedSupervisorsOlderThanWithSupervisorTerminatedAndOlderThanTimeShouldBeDeleted()
  {
    final String supervisor1 = "test-supervisor-1";
    final String datasource1 = "datasource-1";
    final Map<String, String> data1rev1 = ImmutableMap.of("key1-1", "value1-1-1", "key1-2", "value1-2-1");
    Assert.assertTrue(supervisorManager.getAll().isEmpty());
    supervisorManager.insert(supervisor1, new TestSupervisorSpec(supervisor1, data1rev1));
    supervisorManager.insert(supervisor1, new NoopSupervisorSpec(supervisor1, ImmutableList.of(datasource1)));
    // Test that supervisor was inserted
    Map<String, List<VersionedSupervisorSpec>> supervisorSpecs = supervisorManager.getAll();
    Assert.assertEquals(1, supervisorSpecs.size());
    Assert.assertEquals(2, supervisorSpecs.get(supervisor1).size());
    Map<String, SupervisorSpec> latestSpecs = supervisorManager.getLatest();
    Assert.assertEquals(1, latestSpecs.size());
    Assert.assertEquals(ImmutableList.of(datasource1), ((NoopSupervisorSpec) latestSpecs.get(supervisor1)).getDataSources());
    // Do delete. Supervisor should be deleted as it is terminated
    int deleteCount = supervisorManager.removeTerminatedSupervisorsOlderThan(System.currentTimeMillis());
    // Verify that supervisor was actually deleted
    Assert.assertEquals(2, deleteCount);
    supervisorSpecs = supervisorManager.getAll();
    Assert.assertEquals(0, supervisorSpecs.size());
    latestSpecs = supervisorManager.getLatest();
    Assert.assertEquals(0, latestSpecs.size());
  }

  @Test
  public void testRemoveTerminatedSupervisorsOlderThanWithSupervisorTerminatedButNotOlderThanTimeShouldNotBeDeleted()
  {
    final String supervisor1 = "test-supervisor-1";
    final String datasource1 = "datasource-1";
    final Map<String, String> data1rev1 = ImmutableMap.of("key1-1", "value1-1-1", "key1-2", "value1-2-1");
    Assert.assertTrue(supervisorManager.getAll().isEmpty());
    supervisorManager.insert(supervisor1, new TestSupervisorSpec(supervisor1, data1rev1));
    supervisorManager.insert(supervisor1, new NoopSupervisorSpec(supervisor1, ImmutableList.of(datasource1)));
    // Test that supervisor was inserted
    Map<String, List<VersionedSupervisorSpec>> supervisorSpecs = supervisorManager.getAll();
    Assert.assertEquals(1, supervisorSpecs.size());
    Assert.assertEquals(2, supervisorSpecs.get(supervisor1).size());
    Map<String, SupervisorSpec> latestSpecs = supervisorManager.getLatest();
    Assert.assertEquals(1, latestSpecs.size());
    Assert.assertEquals(ImmutableList.of(datasource1), ((NoopSupervisorSpec) latestSpecs.get(supervisor1)).getDataSources());
    // Do delete. Supervisor should not be deleted. Supervisor is terminated but it was created just now so it's
    // created timestamp will be later than the timestamp 2012-01-01T00:00:00Z
    int deleteCount = supervisorManager.removeTerminatedSupervisorsOlderThan(DateTimes.of("2012-01-01T00:00:00Z").getMillis());
    // Verify that supervisor was not deleted
    Assert.assertEquals(0, deleteCount);
    supervisorSpecs = supervisorManager.getAll();
    Assert.assertEquals(1, supervisorSpecs.size());
    Assert.assertEquals(2, supervisorSpecs.get(supervisor1).size());
    latestSpecs = supervisorManager.getLatest();
    Assert.assertEquals(1, latestSpecs.size());
    Assert.assertEquals(ImmutableList.of(datasource1), ((NoopSupervisorSpec) latestSpecs.get(supervisor1)).getDataSources());
  }

  @Test
  public void testInsertAndGet()
  {
    final String supervisor1 = "test-supervisor-1";
    final String supervisor2 = "test-supervisor-2";
    final Map<String, String> data1rev1 = ImmutableMap.of("key1-1", "value1-1-1", "key1-2", "value1-2-1");
    final Map<String, String> data1rev2 = ImmutableMap.of("key1-1", "value1-1-2", "key1-2", "value1-2-2");
    final Map<String, String> data1rev3 = ImmutableMap.of("key1-1", "value1-1-3", "key1-2", "value1-2-3");
    final Map<String, String> data2rev1 = ImmutableMap.of("key2-1", "value2-1-1", "key2-2", "value2-2-1");
    final Map<String, String> data2rev2 = ImmutableMap.of("key2-3", "value2-3-2", "key2-4", "value2-4-2");

    Assert.assertTrue(supervisorManager.getAll().isEmpty());

    // add 2 supervisors, one revision each, and make sure the state is as expected
    supervisorManager.insert(supervisor1, new TestSupervisorSpec(supervisor1, data1rev1));
    supervisorManager.insert(supervisor2, new TestSupervisorSpec(supervisor2, data2rev1));

    Map<String, List<VersionedSupervisorSpec>> supervisorSpecs = supervisorManager.getAll();
    Map<String, SupervisorSpec> latestSpecs = supervisorManager.getLatest();
    Assert.assertEquals(2, supervisorSpecs.size());
    Assert.assertEquals(1, supervisorSpecs.get(supervisor1).size());
    Assert.assertEquals(1, supervisorSpecs.get(supervisor2).size());
    Assert.assertEquals(supervisor1, supervisorSpecs.get(supervisor1).get(0).getSpec().getId());
    Assert.assertEquals(supervisor2, supervisorSpecs.get(supervisor2).get(0).getSpec().getId());
    Assert.assertEquals(data1rev1, ((TestSupervisorSpec) supervisorSpecs.get(supervisor1).get(0).getSpec()).getData());
    Assert.assertEquals(data2rev1, ((TestSupervisorSpec) supervisorSpecs.get(supervisor2).get(0).getSpec()).getData());
    Assert.assertEquals(2, latestSpecs.size());
    Assert.assertEquals(data1rev1, ((TestSupervisorSpec) latestSpecs.get(supervisor1)).getData());
    Assert.assertEquals(data2rev1, ((TestSupervisorSpec) latestSpecs.get(supervisor2)).getData());

    // add more revisions to the supervisors
    supervisorManager.insert(supervisor1, new TestSupervisorSpec(supervisor1, data1rev2));
    supervisorManager.insert(supervisor1, new TestSupervisorSpec(supervisor1, data1rev3));
    supervisorManager.insert(supervisor2, new TestSupervisorSpec(supervisor2, data2rev2));

    supervisorSpecs = supervisorManager.getAll();
    latestSpecs = supervisorManager.getLatest();
    Assert.assertEquals(2, supervisorSpecs.size());
    Assert.assertEquals(3, supervisorSpecs.get(supervisor1).size());
    Assert.assertEquals(2, supervisorSpecs.get(supervisor2).size());

    // make sure getAll() returns each spec in descending order
    Assert.assertEquals(data1rev3, ((TestSupervisorSpec) supervisorSpecs.get(supervisor1).get(0).getSpec()).getData());
    Assert.assertEquals(data1rev2, ((TestSupervisorSpec) supervisorSpecs.get(supervisor1).get(1).getSpec()).getData());
    Assert.assertEquals(data1rev1, ((TestSupervisorSpec) supervisorSpecs.get(supervisor1).get(2).getSpec()).getData());
    Assert.assertEquals(data2rev2, ((TestSupervisorSpec) supervisorSpecs.get(supervisor2).get(0).getSpec()).getData());
    Assert.assertEquals(data2rev1, ((TestSupervisorSpec) supervisorSpecs.get(supervisor2).get(1).getSpec()).getData());

    // make sure getLatest() returns the last revision
    Assert.assertEquals(data1rev3, ((TestSupervisorSpec) latestSpecs.get(supervisor1)).getData());
    Assert.assertEquals(data2rev2, ((TestSupervisorSpec) latestSpecs.get(supervisor2)).getData());
  }

  @Test
  public void testSkipDeserializingBadSpecs()
  {
    final String supervisor1 = "test-supervisor-1";
    final String supervisor2 = "test-supervisor-2";
    final Map<String, String> data1rev1 = ImmutableMap.of("key1-1", "value1-1-1", "key1-2", "value1-2-1");
    final Map<String, String> data1rev2 = ImmutableMap.of("key1-1", "value1-1-2", "key1-2", "value1-2-2");

    supervisorManager.insert(supervisor1, new TestSupervisorSpec(supervisor1, data1rev1));
    supervisorManager.insert(supervisor2, new BadSupervisorSpec(supervisor2, supervisor2));
    supervisorManager.insert(supervisor1, new TestSupervisorSpec(supervisor1, data1rev2));

    final Map<String, List<VersionedSupervisorSpec>> allSpecs = supervisorManager.getAll();

    Assert.assertEquals(2, allSpecs.size());
    List<VersionedSupervisorSpec> specs = allSpecs.get(supervisor1);
    Assert.assertEquals(2, specs.size());
    Assert.assertEquals(new TestSupervisorSpec(supervisor1, data1rev2), specs.get(0).getSpec());
    Assert.assertEquals(new TestSupervisorSpec(supervisor1, data1rev1), specs.get(1).getSpec());

    specs = allSpecs.get(supervisor2);
    Assert.assertEquals(1, specs.size());
    Assert.assertNull(specs.get(0).getSpec());
  }

  @Test
  public void testGetLatestActiveOnly()
  {
    final String supervisor1 = "test-supervisor-1";
    final String datasource1 = "datasource-1";
    final String supervisor2 = "test-supervisor-2";
    final Map<String, String> data1rev1 = ImmutableMap.of("key1-1", "value1-1-1", "key1-2", "value1-2-1");
    Assert.assertTrue(supervisorManager.getAll().isEmpty());
    supervisorManager.insert(supervisor1, new TestSupervisorSpec(supervisor1, data1rev1));
    // supervisor1 is terminated
    supervisorManager.insert(supervisor1, new NoopSupervisorSpec(supervisor1, ImmutableList.of(datasource1)));
    // supervisor2 is still active
    supervisorManager.insert(supervisor2, new TestSupervisorSpec(supervisor2, data1rev1));
    // get latest active should only return supervisor2
    Map<String, SupervisorSpec> actual = supervisorManager.getLatestActiveOnly();
    Assert.assertEquals(1, actual.size());
    Assert.assertTrue(actual.containsKey(supervisor2));
  }


  @Test
  public void testGetLatestTerminatedOnly()
  {
    final String supervisor1 = "test-supervisor-1";
    final String datasource1 = "datasource-1";
    final String supervisor2 = "test-supervisor-2";
    final Map<String, String> data1rev1 = ImmutableMap.of("key1-1", "value1-1-1", "key1-2", "value1-2-1");
    Assert.assertTrue(supervisorManager.getAll().isEmpty());
    supervisorManager.insert(supervisor1, new TestSupervisorSpec(supervisor1, data1rev1));
    // supervisor1 is terminated
    supervisorManager.insert(supervisor1, new NoopSupervisorSpec(supervisor1, ImmutableList.of(datasource1)));
    // supervisor2 is still active
    supervisorManager.insert(supervisor2, new TestSupervisorSpec(supervisor2, data1rev1));
    // get latest terminated should only return supervisor1
    Map<String, SupervisorSpec> actual = supervisorManager.getLatestTerminatedOnly();
    Assert.assertEquals(1, actual.size());
    Assert.assertTrue(actual.containsKey(supervisor1));
  }


  private static class BadSupervisorSpec implements SupervisorSpec
  {
    private final String id;
    private final String dataSource;

    private BadSupervisorSpec(String id, String dataSource)
    {
      this.id = id;
      this.dataSource = dataSource;
    }

    @Override
    public String getId()
    {
      return id;
    }

    @Override
    public Supervisor createSupervisor()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public SupervisorTaskAutoScaler createAutoscaler(Supervisor supervisor)
    {
      return null;
    }

    @Override
    public List<String> getDataSources()
    {
      return Collections.singletonList(dataSource);
    }

    @Override
    public String getType()
    {
      return null;
    }

    @Override
    public String getSource()
    {
      return null;
    }

  }
}
