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

package org.apache.druid.indexing.overlord.autoscaling.gce;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceGroupManagersDeleteInstancesRequest;
import com.google.api.services.compute.model.InstanceGroupManagersListManagedInstancesResponse;
import com.google.api.services.compute.model.InstanceList;
import com.google.api.services.compute.model.ManagedInstance;
import com.google.api.services.compute.model.NetworkInterface;
import com.google.api.services.compute.model.Operation;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.indexing.overlord.autoscaling.AutoScaler;
import org.apache.druid.indexing.overlord.autoscaling.AutoScalingData;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 */
public class GceAutoScalerTest
{
  private Compute mockCompute = null;
  // id -> ip & ip -> id
  private Compute.Instances mockInstances = null;
  private Compute.Instances.List mockIpToIdRequest = null;
  private Compute.Instances.List mockIdToIpRequest = null;
  // running instances
  private Compute.InstanceGroupManagers mockInstanceGroupManagers = null;
  private Compute.InstanceGroupManagers.ListManagedInstances mockInstancesRequest = null;
  // terminate
  private Compute.InstanceGroupManagers.DeleteInstances mockDeleteRequest = null;
  //provision
  private Compute.InstanceGroupManagers.Resize mockResizeRequest = null;

  @Before
  public void setUp()
  {
    // for every test let's create all (only a subset needed for each test tho)

    mockCompute = EasyMock.createMock(Compute.class);

    mockInstances = EasyMock.createMock(Compute.Instances.class);
    mockIpToIdRequest = EasyMock.createMock(Compute.Instances.List.class);
    mockIdToIpRequest = EasyMock.createMock(Compute.Instances.List.class);

    mockInstanceGroupManagers = EasyMock.createMock(Compute.InstanceGroupManagers.class);
    mockInstancesRequest = EasyMock.createMock(
            Compute.InstanceGroupManagers.ListManagedInstances.class
    );

    mockDeleteRequest = EasyMock.createMock(Compute.InstanceGroupManagers.DeleteInstances.class);

    mockResizeRequest = EasyMock.createMock(Compute.InstanceGroupManagers.Resize.class);
  }

  @After
  public void tearDown()
  {
    // not calling verify here as we use different bits and pieces in each test
  }

  private static void verifyAutoScaler(final GceAutoScaler autoScaler)
  {
    Assert.assertEquals(1, autoScaler.getEnvConfig().getNumInstances());
    Assert.assertEquals(4, autoScaler.getMaxNumWorkers());
    Assert.assertEquals(2, autoScaler.getMinNumWorkers());
    Assert.assertEquals("winkie-country", autoScaler.getEnvConfig().getZoneName());
    Assert.assertEquals("super-project", autoScaler.getEnvConfig().getProjectId());
    Assert.assertEquals("druid-mig", autoScaler.getEnvConfig().getManagedInstanceGroupName());
  }

  @Test
  public void testConfig()
  {
    final String json = "{\n"
            + "   \"envConfig\" : {\n"
            + "      \"numInstances\" : 1,\n"
            + "      \"projectId\" : \"super-project\",\n"
            + "      \"zoneName\" : \"winkie-country\",\n"
            + "      \"managedInstanceGroupName\" : \"druid-mig\"\n"
            + "   },\n"
            + "   \"maxNumWorkers\" : 4,\n"
            + "   \"minNumWorkers\" : 2,\n"
            + "   \"type\" : \"gce\"\n"
            + "}";

    final ObjectMapper objectMapper = new DefaultObjectMapper()
            .registerModules((Iterable<Module>) new GceModule().getJacksonModules());
    objectMapper.setInjectableValues(
          new InjectableValues()
          {
            @Override
            public Object findInjectableValue(
                    Object o,
                    DeserializationContext deserializationContext,
                    BeanProperty beanProperty,
                    Object o1
            )
            {
              return null;
            }
          }
    );

    try {
      final GceAutoScaler autoScaler =
              (GceAutoScaler) objectMapper.readValue(json, AutoScaler.class);
      verifyAutoScaler(autoScaler);

      final GceAutoScaler roundTripAutoScaler = (GceAutoScaler) objectMapper.readValue(
              objectMapper.writeValueAsBytes(autoScaler),
              AutoScaler.class
      );
      verifyAutoScaler(roundTripAutoScaler);

      Assert.assertEquals("Round trip equals", autoScaler, roundTripAutoScaler);
    }
    catch (Exception e) {
      Assert.fail(StringUtils.format("Got exception in test %s", e.getMessage()));
    }
  }

  @Test
  public void testConfigEquals()
  {
    EqualsVerifier.forClass(GceEnvironmentConfig.class).withNonnullFields(
        "projectId", "zoneName", "managedInstanceGroupName", "numInstances"
    ).usingGetClass().verify();
  }

  private Instance makeInstance(String name, String ip)
  {
    Instance instance = new Instance();
    instance.setName(name);
    NetworkInterface net = new NetworkInterface();
    net.setNetworkIP(ip);
    instance.setNetworkInterfaces(Collections.singletonList(net));
    return instance;
  }

  @Test
  public void testIpToId()
      throws IOException, GeneralSecurityException, GceServiceException
  {
    GceAutoScaler autoScaler = EasyMock.createMockBuilder(GceAutoScaler.class).withConstructor(
        int.class,
        int.class,
        GceEnvironmentConfig.class
    ).withArgs(
        2,
        4,
        new GceEnvironmentConfig(1, "proj-x", "us-central-1", "druid-mig")
    ).addMockedMethod(
        "createComputeServiceImpl"
    ).createMock();

    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(null);
    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(mockCompute);
    EasyMock.replay(autoScaler);

    // empty IPs
    List<String> ips1 = Collections.emptyList();
    List<String> ids1 = autoScaler.ipToIdLookup(ips1);
    Assert.assertEquals(0, ids1.size());

    // actually not IPs
    List<String> ips2 = Collections.singletonList("foo-bar-baz");
    List<String> ids2 = autoScaler.ipToIdLookup(ips2);
    Assert.assertEquals(ips2, ids2);

    // actually IPs
    Instance i1 = makeInstance("foo", "1.2.3.5"); // not the one we look for
    Instance i2 = makeInstance("bar", "1.2.3.4"); // the one we do look for
    InstanceList mockResponse = new InstanceList();
    mockResponse.setNextPageToken(null);
    mockResponse.setItems(Arrays.asList(i1, i2));

    EasyMock.expect(mockIpToIdRequest.execute()).andReturn(mockResponse);
    EasyMock.expect(mockIpToIdRequest.setPageToken(EasyMock.anyString())).andReturn(
        mockIpToIdRequest // the method needs to return something, what is actually irrelevant here
    );
    EasyMock.replay(mockIpToIdRequest);

    EasyMock.expect(mockInstances.list("proj-x", "us-central-1")).andReturn(mockIpToIdRequest);
    EasyMock.replay(mockInstances);

    EasyMock.expect(mockCompute.instances()).andReturn(mockInstances);
    EasyMock.replay(mockCompute);

    List<String> ips3 = Collections.singletonList("1.2.3.4");
    List<String> ids3 = autoScaler.ipToIdLookup(ips3);
    Assert.assertEquals(1, ids3.size());
    Assert.assertEquals("bar", ids3.get(0));

    EasyMock.verify(mockCompute);
    EasyMock.verify(mockInstances);
    EasyMock.verify(mockIpToIdRequest);
  }

  @Test
  public void testIdToIp()
      throws IOException, GeneralSecurityException, GceServiceException
  {
    GceAutoScaler autoScaler = EasyMock.createMockBuilder(GceAutoScaler.class).withConstructor(
        int.class,
        int.class,
        GceEnvironmentConfig.class
    ).withArgs(
        2,
        4,
        new GceEnvironmentConfig(1, "proj-x", "us-central-1", "druid-mig")
    ).addMockedMethod(
        "createComputeServiceImpl"
    ).createMock();

    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(null);
    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(mockCompute);
    EasyMock.replay(autoScaler);

    // empty IPs
    List<String> ids1 = Collections.emptyList();
    List<String> ips1 = autoScaler.idToIpLookup(ids1);
    Assert.assertEquals(0, ips1.size());

    // actually IDs
    Instance i1 = makeInstance("foo", "null");    // invalid ip, not returned
    Instance i2 = makeInstance("bar", "1.2.3.4"); // valid ip, returned
    InstanceList mockResponse = new InstanceList();
    mockResponse.setNextPageToken(null);
    mockResponse.setItems(Arrays.asList(i1, i2));

    EasyMock.expect(mockIdToIpRequest.setFilter("(name = \"foo\") OR (name = \"bar\")")).andReturn(
        mockIdToIpRequest // the method needs to return something but it is actually irrelevant
    );
    EasyMock.expect(mockIdToIpRequest.execute()).andReturn(mockResponse);
    EasyMock.expect(mockIdToIpRequest.setPageToken(EasyMock.anyString())).andReturn(
        mockIdToIpRequest // the method needs to return something but it is actually irrelevant
    );
    EasyMock.replay(mockIdToIpRequest);

    EasyMock.expect(mockInstances.list("proj-x", "us-central-1")).andReturn(mockIdToIpRequest);
    EasyMock.replay(mockInstances);

    EasyMock.expect(mockCompute.instances()).andReturn(mockInstances);
    EasyMock.replay(mockCompute);

    List<String> ids3 = Arrays.asList("foo", "bar");
    List<String> ips3 = autoScaler.idToIpLookup(ids3);
    Assert.assertEquals(1, ips3.size());
    Assert.assertEquals("1.2.3.4", ips3.get(0));

    EasyMock.verify(mockCompute);
    EasyMock.verify(mockInstances);
    EasyMock.verify(mockIdToIpRequest);
  }

  private InstanceGroupManagersListManagedInstancesResponse createRunningInstances(
      List<String> instances
  )
  {
    InstanceGroupManagersListManagedInstancesResponse mockResponse =
        new InstanceGroupManagersListManagedInstancesResponse();
    mockResponse.setManagedInstances(new ArrayList<>());
    for (String x : instances) {
      ManagedInstance mi = new ManagedInstance();
      mi.setInstance(x);
      mockResponse.getManagedInstances().add(mi);
    }
    return mockResponse;
  }

  @Test
  public void testTerminateWithIds()
      throws IOException, GeneralSecurityException, GceServiceException
  {
    GceAutoScaler autoScaler = EasyMock.createMockBuilder(GceAutoScaler.class).withConstructor(
        int.class,
        int.class,
        GceEnvironmentConfig.class
    ).withArgs(
        2,
        4,
        new GceEnvironmentConfig(1, "proj-x", "us-central-1", "druid-mig")
    ).addMockedMethod(
        "createComputeServiceImpl"
    ).createMock();

    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(null);
    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(mockCompute);
    EasyMock.replay(autoScaler);

    // set up getRunningInstances results
    InstanceGroupManagersListManagedInstancesResponse beforeRunningInstance =
        createRunningInstances(Arrays.asList(
            "http://xyz/foo",
            "http://xyz/bar",
            "http://xyz/baz"
        ));
    InstanceGroupManagersListManagedInstancesResponse afterRunningInstance =
        createRunningInstances(Arrays.asList(
            "http://xyz/foo",
            "http://xyz/bar"
        ));

    EasyMock.expect(mockInstancesRequest.execute()).andReturn(beforeRunningInstance); // 1st call
    EasyMock.expect(mockInstancesRequest.setMaxResults(500L)).andReturn(mockInstancesRequest);
    EasyMock.expect(mockInstancesRequest.execute()).andReturn(afterRunningInstance);  // 2nd call
    EasyMock.expect(mockInstancesRequest.setMaxResults(500L)).andReturn(mockInstancesRequest);
    EasyMock.replay(mockInstancesRequest);


    EasyMock.expect(mockInstanceGroupManagers.listManagedInstances(
            "proj-x",
            "us-central-1",
            "druid-mig"
            )).andReturn(mockInstancesRequest).times(2);

    // set up the delete operation
    Operation mockResponse = new Operation();
    mockResponse.setStatus("DONE");
    mockResponse.setError(new Operation.Error());

    EasyMock.expect(mockDeleteRequest.execute()).andReturn(mockResponse);
    EasyMock.replay(mockDeleteRequest);

    InstanceGroupManagersDeleteInstancesRequest requestBody =
            new InstanceGroupManagersDeleteInstancesRequest();
    requestBody.setInstances(Collections.singletonList("zones/us-central-1/instances/baz"));

    EasyMock.expect(mockInstanceGroupManagers.deleteInstances(
            "proj-x",
            "us-central-1",
            "druid-mig",
            requestBody
    )).andReturn(mockDeleteRequest);

    EasyMock.replay(mockInstanceGroupManagers);

    // called twice in getRunningInstances...
    EasyMock.expect(mockCompute.instanceGroupManagers()).andReturn(mockInstanceGroupManagers);
    EasyMock.expect(mockCompute.instanceGroupManagers()).andReturn(mockInstanceGroupManagers);
    // ...and once in terminateWithIds
    EasyMock.expect(mockCompute.instanceGroupManagers()).andReturn(mockInstanceGroupManagers);

    // and that's all folks!
    EasyMock.replay(mockCompute);

    AutoScalingData autoScalingData =
            autoScaler.terminateWithIds(Collections.singletonList("baz"));
    Assert.assertEquals(1, autoScalingData.getNodeIds().size());
    Assert.assertEquals("baz", autoScalingData.getNodeIds().get(0));

    EasyMock.verify(mockCompute);
    EasyMock.verify(mockInstanceGroupManagers);
    EasyMock.verify(mockDeleteRequest);
    EasyMock.verify(mockInstancesRequest);
  }

  @Test
  public void testTerminate()
      throws IOException, GeneralSecurityException, GceServiceException
  {
    GceAutoScaler autoScaler = EasyMock.createMockBuilder(GceAutoScaler.class).withConstructor(
        int.class,
        int.class,
        GceEnvironmentConfig.class
    ).withArgs(
        2,
        4,
        new GceEnvironmentConfig(1, "proj-x", "us-central-1", "druid-mig")
    ).addMockedMethod(
        "createComputeServiceImpl"
    ).createMock();

    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(null);
    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(mockCompute);
    EasyMock.replay(autoScaler);

    // testing the ip --> id part
    Instance i0 = makeInstance("baz", "1.2.3.6");
    InstanceList mockInstanceListResponse = new InstanceList();
    mockInstanceListResponse.setNextPageToken(null);
    mockInstanceListResponse.setItems(Collections.singletonList(i0));

    EasyMock.expect(mockIpToIdRequest.execute()).andReturn(mockInstanceListResponse);
    EasyMock.expect(mockIpToIdRequest.setPageToken(EasyMock.anyString())).andReturn(
        mockIpToIdRequest // the method needs to return something, what is actually irrelevant here
    );
    EasyMock.replay(mockIpToIdRequest);

    EasyMock.expect(mockInstances.list("proj-x", "us-central-1")).andReturn(mockIpToIdRequest);

    EasyMock.expect(mockCompute.instances()).andReturn(mockInstances);
    EasyMock.replay(mockInstances);

    // testing the delete part
    InstanceGroupManagersListManagedInstancesResponse beforeRunningInstance =
        createRunningInstances(Arrays.asList(
            "http://xyz/foo",
            "http://xyz/bar",
            "http://xyz/baz"
        ));
    InstanceGroupManagersListManagedInstancesResponse afterRunningInstance =
        createRunningInstances(Arrays.asList(
            "http://xyz/foo",
            "http://xyz/bar"
        ));

    EasyMock.expect(mockInstancesRequest.execute()).andReturn(beforeRunningInstance); // 1st call
    EasyMock.expect(mockInstancesRequest.setMaxResults(500L)).andReturn(mockInstancesRequest);
    EasyMock.expect(mockInstancesRequest.execute()).andReturn(afterRunningInstance);  // 2nd call
    EasyMock.expect(mockInstancesRequest.setMaxResults(500L)).andReturn(mockInstancesRequest);
    EasyMock.replay(mockInstancesRequest);

    EasyMock.expect(mockInstanceGroupManagers.listManagedInstances(
        "proj-x",
        "us-central-1",
        "druid-mig"
    )).andReturn(mockInstancesRequest).times(2);

    // set up the delete operation
    Operation mockResponse = new Operation();
    mockResponse.setStatus("DONE");
    mockResponse.setError(new Operation.Error());

    EasyMock.expect(mockDeleteRequest.execute()).andReturn(mockResponse);
    EasyMock.replay(mockDeleteRequest);

    InstanceGroupManagersDeleteInstancesRequest requestBody =
        new InstanceGroupManagersDeleteInstancesRequest();
    requestBody.setInstances(Collections.singletonList("zones/us-central-1/instances/baz"));

    EasyMock.expect(mockInstanceGroupManagers.deleteInstances(
        "proj-x",
        "us-central-1",
        "druid-mig",
        requestBody
    )).andReturn(mockDeleteRequest);

    EasyMock.replay(mockInstanceGroupManagers);

    // called twice in getRunningInstances...
    EasyMock.expect(mockCompute.instanceGroupManagers()).andReturn(mockInstanceGroupManagers);
    EasyMock.expect(mockCompute.instanceGroupManagers()).andReturn(mockInstanceGroupManagers);
    // ...and once in terminateWithIds
    EasyMock.expect(mockCompute.instanceGroupManagers()).andReturn(mockInstanceGroupManagers);

    // and that's all folks!
    EasyMock.replay(mockCompute);

    AutoScalingData autoScalingData =
        autoScaler.terminate(Collections.singletonList("1.2.3.6"));
    Assert.assertEquals(1, autoScalingData.getNodeIds().size());
    Assert.assertEquals("baz", autoScalingData.getNodeIds().get(0));

    EasyMock.verify(mockCompute);
    EasyMock.verify(mockIpToIdRequest);
    EasyMock.verify(mockInstanceGroupManagers);
    EasyMock.verify(mockDeleteRequest);
    EasyMock.verify(mockInstancesRequest);
  }

  @Test
  public void testTerminateWithIdsWithMissingRemoval()
      throws IOException, GeneralSecurityException, GceServiceException
  {
    GceAutoScaler autoScaler = EasyMock.createMockBuilder(GceAutoScaler.class).withConstructor(
        int.class,
        int.class,
        GceEnvironmentConfig.class
    ).withArgs(
        2,
        4,
        new GceEnvironmentConfig(1, "proj-x", "us-central-1", "druid-mig")
    ).addMockedMethod(
        "createComputeServiceImpl"
    ).createMock();

    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(null);
    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(mockCompute);
    EasyMock.replay(autoScaler);

    // set up getRunningInstances results
    InstanceGroupManagersListManagedInstancesResponse beforeRunningInstance =
        createRunningInstances(Arrays.asList(
            "http://xyz/foo",
            "http://xyz/bar",
            "http://xyz/baz"
        ));
    InstanceGroupManagersListManagedInstancesResponse after1RunningInstance =
        createRunningInstances(Arrays.asList(
            "http://xyz/foo",
            "http://xyz/bar",
            "http://xyz/baz"
        )); // not changing anything, will trigger the loop around getRunningInstances
    InstanceGroupManagersListManagedInstancesResponse after2RunningInstance =
        createRunningInstances(Arrays.asList(
            "http://xyz/foo",
            "http://xyz/bar"
        )); // now the machine got dropped!

    EasyMock.expect(mockInstancesRequest.execute()).andReturn(beforeRunningInstance); // 1st call
    EasyMock.expect(mockInstancesRequest.setMaxResults(500L)).andReturn(mockInstancesRequest);
    EasyMock.expect(mockInstancesRequest.execute()).andReturn(after1RunningInstance); // 2nd call, the next is needed
    EasyMock.expect(mockInstancesRequest.setMaxResults(500L)).andReturn(mockInstancesRequest);
    EasyMock.expect(mockInstancesRequest.execute()).andReturn(after2RunningInstance); // 3rd call, this unblocks
    EasyMock.expect(mockInstancesRequest.setMaxResults(500L)).andReturn(mockInstancesRequest);
    EasyMock.replay(mockInstancesRequest);


    EasyMock.expect(mockInstanceGroupManagers.listManagedInstances(
        "proj-x",
        "us-central-1",
        "druid-mig"
    )).andReturn(mockInstancesRequest).times(3);

    // set up the delete operation
    Operation mockResponse = new Operation();
    mockResponse.setStatus("DONE");
    mockResponse.setError(new Operation.Error());

    EasyMock.expect(mockDeleteRequest.execute()).andReturn(mockResponse);
    EasyMock.replay(mockDeleteRequest);

    InstanceGroupManagersDeleteInstancesRequest requestBody =
        new InstanceGroupManagersDeleteInstancesRequest();
    requestBody.setInstances(Collections.singletonList("zones/us-central-1/instances/baz"));

    EasyMock.expect(mockInstanceGroupManagers.deleteInstances(
        "proj-x",
        "us-central-1",
        "druid-mig",
        requestBody
    )).andReturn(mockDeleteRequest);

    EasyMock.replay(mockInstanceGroupManagers);

    // called three times in getRunningInstances...
    EasyMock.expect(mockCompute.instanceGroupManagers()).andReturn(mockInstanceGroupManagers);
    EasyMock.expect(mockCompute.instanceGroupManagers()).andReturn(mockInstanceGroupManagers);
    EasyMock.expect(mockCompute.instanceGroupManagers()).andReturn(mockInstanceGroupManagers);
    // ...and once in terminateWithIds
    EasyMock.expect(mockCompute.instanceGroupManagers()).andReturn(mockInstanceGroupManagers);

    // and that's all folks!
    EasyMock.replay(mockCompute);

    AutoScalingData autoScalingData =
        autoScaler.terminateWithIds(Collections.singletonList("baz"));
    Assert.assertEquals(1, autoScalingData.getNodeIds().size());
    Assert.assertEquals("baz", autoScalingData.getNodeIds().get(0));

    EasyMock.verify(mockCompute);
    EasyMock.verify(mockInstanceGroupManagers);
    EasyMock.verify(mockDeleteRequest);
    EasyMock.verify(mockInstancesRequest);
  }

  @Test
  public void testProvision()
      throws IOException, GeneralSecurityException, GceServiceException
  {
    GceAutoScaler autoScaler = EasyMock.createMockBuilder(GceAutoScaler.class).withConstructor(
        int.class,
        int.class,
        GceEnvironmentConfig.class
    ).withArgs(
        2,
        4,
        new GceEnvironmentConfig(1, "proj-x", "us-central-1", "druid-mig")
    ).addMockedMethod(
        "createComputeServiceImpl"
    ).createMock();

    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(null);
    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(mockCompute);
    EasyMock.replay(autoScaler);

    // set up getRunningInstances results
    InstanceGroupManagersListManagedInstancesResponse beforeRunningInstance =
            createRunningInstances(Arrays.asList(
                    "http://xyz/foo",
                    "http://xyz/bar"
            ));
    InstanceGroupManagersListManagedInstancesResponse afterRunningInstance =
            createRunningInstances(Arrays.asList(
                    "http://xyz/foo",
                    "http://xyz/bar",
                    "http://xyz/baz"
            ));

    EasyMock.expect(mockInstancesRequest.execute()).andReturn(beforeRunningInstance); // 1st call
    EasyMock.expect(mockInstancesRequest.setMaxResults(500L)).andReturn(mockInstancesRequest);
    EasyMock.expect(mockInstancesRequest.execute()).andReturn(afterRunningInstance);  // 2nd call
    EasyMock.expect(mockInstancesRequest.setMaxResults(500L)).andReturn(mockInstancesRequest);
    EasyMock.replay(mockInstancesRequest);

    EasyMock.expect(mockInstanceGroupManagers.listManagedInstances(
            "proj-x",
            "us-central-1",
            "druid-mig"
    )).andReturn(mockInstancesRequest).times(2);

    // set up the resize operation
    Operation mockResponse = new Operation();
    mockResponse.setStatus("DONE");
    mockResponse.setError(new Operation.Error());

    EasyMock.expect(mockResizeRequest.execute()).andReturn(mockResponse);
    EasyMock.replay(mockResizeRequest);

    EasyMock.expect(mockInstanceGroupManagers.resize(
            "proj-x",
            "us-central-1",
            "druid-mig",
            3
    )).andReturn(mockResizeRequest);

    EasyMock.replay(mockInstanceGroupManagers);

    // called twice in getRunningInstances...
    EasyMock.expect(mockCompute.instanceGroupManagers()).andReturn(mockInstanceGroupManagers);
    EasyMock.expect(mockCompute.instanceGroupManagers()).andReturn(mockInstanceGroupManagers);
    // ...and once in provision
    EasyMock.expect(mockCompute.instanceGroupManagers()).andReturn(mockInstanceGroupManagers);

    // and that's all folks!
    EasyMock.replay(mockCompute);

    AutoScalingData autoScalingData = autoScaler.provision();
    Assert.assertEquals(1, autoScalingData.getNodeIds().size());
    Assert.assertEquals("baz", autoScalingData.getNodeIds().get(0));

    EasyMock.verify(mockCompute);
    EasyMock.verify(mockInstanceGroupManagers);
    EasyMock.verify(mockResizeRequest);
    EasyMock.verify(mockInstancesRequest);
  }

  @Test
  public void testProvisionSkipped()
      throws IOException, GeneralSecurityException, GceServiceException
  {
    GceAutoScaler autoScaler = EasyMock.createMockBuilder(GceAutoScaler.class).withConstructor(
        int.class,
        int.class,
        GceEnvironmentConfig.class
    ).withArgs(
        2,
        4,
        new GceEnvironmentConfig(1, "proj-x", "us-central-1", "druid-mig")
    ).addMockedMethod(
        "createComputeServiceImpl"
    ).createMock();

    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(null);
    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(mockCompute);
    EasyMock.replay(autoScaler);

    // set up getRunningInstances results
    InstanceGroupManagersListManagedInstancesResponse beforeRunningInstance =
            createRunningInstances(Arrays.asList(
                    "http://xyz/foo",
                    "http://xyz/bar",
                    "http://xyz/baz",
                    "http://xyz/zab" // already max instances, will not scale
            ));

    EasyMock.expect(mockInstancesRequest.execute()).andReturn(beforeRunningInstance);
    EasyMock.expect(mockInstancesRequest.setMaxResults(500L)).andReturn(mockInstancesRequest);
    EasyMock.replay(mockInstancesRequest);

    EasyMock.expect(mockInstanceGroupManagers.listManagedInstances(
            "proj-x",
            "us-central-1",
            "druid-mig"
    )).andReturn(mockInstancesRequest);

    EasyMock.expect(mockCompute.instanceGroupManagers()).andReturn(mockInstanceGroupManagers);
    EasyMock.replay(mockInstanceGroupManagers);

    // and that's all folks!
    EasyMock.replay(mockCompute);

    AutoScalingData autoScalingData = autoScaler.provision();
    Assert.assertEquals(0, autoScalingData.getNodeIds().size());

    EasyMock.verify(mockCompute);
    EasyMock.verify(mockInstancesRequest);
    EasyMock.verify(mockInstanceGroupManagers);
  }

  @Test
  public void testProvisionWithMissingNewInstances()
      throws IOException, GeneralSecurityException, GceServiceException
  {
    GceAutoScaler autoScaler = EasyMock.createMockBuilder(GceAutoScaler.class).withConstructor(
        int.class,
        int.class,
        GceEnvironmentConfig.class
    ).withArgs(
        2,
        4,
        new GceEnvironmentConfig(1, "proj-x", "us-central-1", "druid-mig")
    ).addMockedMethod(
        "createComputeServiceImpl"
    ).createMock();

    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(null);
    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(mockCompute);
    EasyMock.replay(autoScaler);

    // set up getRunningInstances results
    InstanceGroupManagersListManagedInstancesResponse beforeRunningInstance =
        createRunningInstances(Arrays.asList(
            "http://xyz/foo",
            "http://xyz/bar"
        ));
    InstanceGroupManagersListManagedInstancesResponse after1RunningInstance =
        createRunningInstances(Arrays.asList(
            "http://xyz/foo",
            "http://xyz/bar"
        )); // not changing anything, will trigger the loop around getRunningInstances
    InstanceGroupManagersListManagedInstancesResponse after2RunningInstance =
        createRunningInstances(Arrays.asList(
            "http://xyz/foo",
            "http://xyz/bar",
            "http://xyz/baz"
        )); // now the new machine is here!

    EasyMock.expect(mockInstancesRequest.execute()).andReturn(beforeRunningInstance); // 1st call
    EasyMock.expect(mockInstancesRequest.setMaxResults(500L)).andReturn(mockInstancesRequest);
    EasyMock.expect(mockInstancesRequest.execute()).andReturn(after1RunningInstance); // 2nd call, the next is needed
    EasyMock.expect(mockInstancesRequest.setMaxResults(500L)).andReturn(mockInstancesRequest);
    EasyMock.expect(mockInstancesRequest.execute()).andReturn(after2RunningInstance); // 3rd call, this unblocks
    EasyMock.expect(mockInstancesRequest.setMaxResults(500L)).andReturn(mockInstancesRequest);
    EasyMock.replay(mockInstancesRequest);

    EasyMock.expect(mockInstanceGroupManagers.listManagedInstances(
        "proj-x",
        "us-central-1",
        "druid-mig"
    )).andReturn(mockInstancesRequest).times(3);

    // set up the resize operation
    Operation mockResponse = new Operation();
    mockResponse.setStatus("DONE");
    mockResponse.setError(new Operation.Error());

    EasyMock.expect(mockResizeRequest.execute()).andReturn(mockResponse);
    EasyMock.replay(mockResizeRequest);

    EasyMock.expect(mockInstanceGroupManagers.resize(
        "proj-x",
        "us-central-1",
        "druid-mig",
        3
    )).andReturn(mockResizeRequest);

    EasyMock.replay(mockInstanceGroupManagers);

    // called three times in getRunningInstances...
    EasyMock.expect(mockCompute.instanceGroupManagers()).andReturn(mockInstanceGroupManagers);
    EasyMock.expect(mockCompute.instanceGroupManagers()).andReturn(mockInstanceGroupManagers);
    EasyMock.expect(mockCompute.instanceGroupManagers()).andReturn(mockInstanceGroupManagers);
    // ...and once in provision
    EasyMock.expect(mockCompute.instanceGroupManagers()).andReturn(mockInstanceGroupManagers);

    // and that's all folks!
    EasyMock.replay(mockCompute);

    AutoScalingData autoScalingData = autoScaler.provision();
    Assert.assertEquals(1, autoScalingData.getNodeIds().size());
    Assert.assertEquals("baz", autoScalingData.getNodeIds().get(0));

    EasyMock.verify(mockCompute);
    EasyMock.verify(mockInstanceGroupManagers);
    EasyMock.verify(mockResizeRequest);
    EasyMock.verify(mockInstancesRequest);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(GceAutoScaler.class).withNonnullFields(
        "envConfig", "maxNumWorkers", "minNumWorkers"
    ).withIgnoredFields("cachedComputeService").usingGetClass().verify();
  }

  @Test
  public void testFailedComputeCreation()
      throws IOException, GeneralSecurityException, GceServiceException
  {
    GceAutoScaler autoScaler = EasyMock.createMockBuilder(GceAutoScaler.class).withConstructor(
        int.class,
        int.class,
        GceEnvironmentConfig.class
    ).withArgs(
        2,
        4,
        new GceEnvironmentConfig(1, "proj-x", "us-central-1", "druid-mig")
    ).addMockedMethod(
        "createComputeServiceImpl"
    ).createMock();

    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(null);
    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(null);
    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(null);
    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(null);
    EasyMock.expect(autoScaler.createComputeServiceImpl()).andReturn(null);
    EasyMock.replay(autoScaler);

    List<String> ips = Collections.singletonList("1.2.3.4");
    List<String> ids = autoScaler.ipToIdLookup(ips);
    Assert.assertEquals(0, ids.size());  // Exception caught in execution results in empty result
  }

}
