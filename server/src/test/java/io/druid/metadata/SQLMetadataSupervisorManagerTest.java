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

package io.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import io.druid.indexing.overlord.supervisor.SupervisorSpec;
import io.druid.indexing.overlord.supervisor.VersionedSupervisorSpec;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.List;
import java.util.Map;

public class SQLMetadataSupervisorManagerTest
{
  private static final ObjectMapper mapper = new DefaultObjectMapper();

  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig;
  private SQLMetadataSupervisorManager supervisorManager;

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  @BeforeClass
  public static void setupStatic()
  {
    mapper.registerSubtypes(TestSupervisorSpec.class);
  }

  @Before
  public void setUp()
  {
    connector = derbyConnectorRule.getConnector();
    tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    connector.createSupervisorsTable();

    supervisorManager = new SQLMetadataSupervisorManager(mapper, connector, Suppliers.ofInstance(tablesConfig));
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

  @After
  public void cleanup()
  {
    connector.getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            handle.createStatement(StringUtils.format("DROP TABLE %s", tablesConfig.getSupervisorTable()))
                  .execute();
            return null;
          }
        }
    );
  }
}
