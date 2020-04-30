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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.ComputeScopes;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceGroupManagersDeleteInstancesRequest;
import com.google.api.services.compute.model.InstanceGroupManagersListManagedInstancesResponse;
import com.google.api.services.compute.model.InstanceList;
import com.google.api.services.compute.model.ManagedInstance;
import com.google.api.services.compute.model.NetworkInterface;
import com.google.api.services.compute.model.Operation;
import com.google.common.base.Preconditions;
import com.google.common.net.InetAddresses;
import org.apache.druid.indexing.overlord.autoscaling.AutoScaler;
import org.apache.druid.indexing.overlord.autoscaling.AutoScalingData;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This module permits the autoscaling of the workers in GCE
 *
 * General notes:
 * - The IPs are IPs as in Internet Protocol, and they look like 1.2.3.4
 * - The IDs are the names of the instances of instances created, they look like prefix-abcd,
 *   where the prefix is chosen by you and abcd is a suffix assigned by GCE
 */
@JsonTypeName("gce")
public class GceAutoScaler implements AutoScaler<GceEnvironmentConfig>
{
  private static final EmittingLogger log = new EmittingLogger(GceAutoScaler.class);

  private final GceEnvironmentConfig envConfig;
  private final int minNumWorkers;
  private final int maxNumWorkers;

  private Compute cachedComputeService = null;

  private static final long POLL_INTERVAL_MS = 5 * 1000;  // 5 sec
  private static final int RUNNING_INSTANCES_MAX_RETRIES = 10;
  private static final int OPERATION_END_MAX_RETRIES = 10;

  @JsonCreator
  public GceAutoScaler(
          @JsonProperty("minNumWorkers") int minNumWorkers,
          @JsonProperty("maxNumWorkers") int maxNumWorkers,
          @JsonProperty("envConfig") GceEnvironmentConfig envConfig
  )
  {
    Preconditions.checkArgument(minNumWorkers > 0,
                                "minNumWorkers must be greater than 0");
    this.minNumWorkers = minNumWorkers;
    Preconditions.checkArgument(maxNumWorkers > 0,
                                "maxNumWorkers must be greater than 0");
    Preconditions.checkArgument(maxNumWorkers > minNumWorkers,
                                "maxNumWorkers must be greater than minNumWorkers");
    this.maxNumWorkers = maxNumWorkers;
    this.envConfig = envConfig;
  }

  @Override
  @JsonProperty
  public int getMinNumWorkers()
  {
    return minNumWorkers;
  }

  @Override
  @JsonProperty
  public int getMaxNumWorkers()
  {
    return maxNumWorkers;
  }

  @Override
  @JsonProperty
  public GceEnvironmentConfig getEnvConfig()
  {
    return envConfig;
  }

  @Nullable
  Compute createComputeServiceImpl()
      throws IOException, GeneralSecurityException, GceServiceException
  {
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
    GoogleCredential credential = GoogleCredential.getApplicationDefault(
        httpTransport,
        jsonFactory
    );
    if (credential.createScopedRequired()) {
      List<String> scopes = new ArrayList<>();
      scopes.add(ComputeScopes.CLOUD_PLATFORM);
      scopes.add(ComputeScopes.COMPUTE);
      credential = credential.createScoped(scopes);
    }

    if (credential.getClientAuthentication() != null) {
      throw new GceServiceException("Not using a service account");
    }

    return new Compute.Builder(httpTransport, jsonFactory, credential)
        .setApplicationName("DruidAutoscaler")
        .build();
  }

  private synchronized Compute createComputeService()
      throws IOException, GeneralSecurityException, InterruptedException, GceServiceException
  {
    final int maxRetries = 5;

    int retries = 0;
    // This retry loop is here to catch the cases in which the underlying call to
    // Compute.Builder(...).build() returns null, case that has been experienced
    // sporadically at start time
    while (cachedComputeService == null && retries < maxRetries) {
      if (retries > 0) {
        Thread.sleep(POLL_INTERVAL_MS);
      }

      log.info("Creating new ComputeService [%d/%d]", retries + 1, maxRetries);

      try {
        cachedComputeService = createComputeServiceImpl();
        retries++;
      }
      catch (Throwable e) {
        log.error(e, "Got Exception in creating the ComputeService");
        throw e;
      }
    }
    return cachedComputeService;
  }

  // Used to wait for an operation to finish
  @Nullable
  private Operation.Error waitForOperationEnd(
      Compute compute,
      Operation operation) throws Exception
  {
    String status = operation.getStatus();
    String opId = operation.getName();
    for (int i = 0; i < OPERATION_END_MAX_RETRIES; i++) {
      if (operation == null || "DONE".equals(status)) {
        return operation == null ? null : operation.getError();
      }
      log.info("Waiting for operation %s to end", opId);
      Thread.sleep(POLL_INTERVAL_MS);
      Compute.ZoneOperations.Get get = compute.zoneOperations().get(
          envConfig.getProjectId(),
          envConfig.getZoneName(),
          opId
      );
      operation = get.execute();
      if (operation != null) {
        status = operation.getStatus();
      }
    }
    throw new InterruptedException(
        StringUtils.format("Timed out waiting for operation %s to complete", opId)
    );
  }

  /**
   * When called resizes envConfig.getManagedInstanceGroupName() increasing it by creating
   * envConfig.getNumInstances() new workers (unless the maximum is reached). Return the
   * IDs of the workers created
   */
  @Override
  public AutoScalingData provision()
  {
    final String project = envConfig.getProjectId();
    final String zone = envConfig.getZoneName();
    final int numInstances = envConfig.getNumInstances();
    final String managedInstanceGroupName = envConfig.getManagedInstanceGroupName();

    try {
      List<String> before = getRunningInstances();
      log.debug("Existing instances [%s]", String.join(",", before));

      int toSize = Math.min(before.size() + numInstances, getMaxNumWorkers());
      if (before.size() >= toSize) {
        // nothing to scale
        return new AutoScalingData(new ArrayList<>());
      }
      log.info("Asked to provision instances, will resize to %d", toSize);

      Compute computeService = createComputeService();
      Compute.InstanceGroupManagers.Resize request =
              computeService.instanceGroupManagers().resize(project, zone,
                      managedInstanceGroupName, toSize);

      Operation response = request.execute();
      Operation.Error err = waitForOperationEnd(computeService, response);
      if (err == null || err.isEmpty()) {
        List<String> after = null;
        // as the waitForOperationEnd only waits for the operation to be scheduled
        // this loop waits until the requested machines actually go up (or up to a
        // certain amount of retries in checking)
        for (int i = 0; i < RUNNING_INSTANCES_MAX_RETRIES; i++) {
          after = getRunningInstances();
          if (after.size() == toSize) {
            break;
          }
          log.info("Machines not up yet, waiting");
          Thread.sleep(POLL_INTERVAL_MS);
        }
        after.removeAll(before); // these should be the new ones
        log.info("Added instances [%s]", String.join(",", after));
        return new AutoScalingData(after);
      } else {
        log.error("Unable to provision instances: %s", err.toPrettyString());
      }
    }
    catch (Exception e) {
      log.error(e, "Unable to provision any gce instances.");
    }

    return new AutoScalingData(new ArrayList<>());
  }

  /**
   * Terminates the instances in the list of IPs provided by the caller
   */
  @Override
  public AutoScalingData terminate(List<String> ips)
  {
    log.info("Asked to terminate: [%s]", String.join(",", ips));

    if (ips.isEmpty()) {
      return new AutoScalingData(new ArrayList<>());
    }

    List<String> nodeIds = ipToIdLookup(ips); // if they are not IPs, they will be unchanged
    try {
      return terminateWithIds(nodeIds != null ? nodeIds : new ArrayList<>());
    }
    catch (Exception e) {
      log.error(e, "Unable to terminate any instances.");
    }

    return new AutoScalingData(new ArrayList<>());
  }

  private List<String> namesToInstances(List<String> names)
  {
    List<String> instances = new ArrayList<>();
    for (String name : names) {
      instances.add(
          // convert the name into a URL's path to be used in calls to the API
          StringUtils.format("zones/%s/instances/%s", envConfig.getZoneName(), name)
      );
    }
    return instances;
  }

  /**
   * Terminates the instances in the list of IDs provided by the caller
   */
  @Override
  public AutoScalingData terminateWithIds(List<String> ids)
  {
    log.info("Asked to terminate IDs: [%s]", String.join(",", ids));

    if (ids.isEmpty()) {
      return new AutoScalingData(new ArrayList<>());
    }

    try {
      final String project = envConfig.getProjectId();
      final String zone = envConfig.getZoneName();
      final String managedInstanceGroupName = envConfig.getManagedInstanceGroupName();

      List<String> before = getRunningInstances();

      InstanceGroupManagersDeleteInstancesRequest requestBody =
              new InstanceGroupManagersDeleteInstancesRequest();
      requestBody.setInstances(namesToInstances(ids));

      Compute computeService = createComputeService();
      Compute.InstanceGroupManagers.DeleteInstances request =
              computeService
                      .instanceGroupManagers()
                      .deleteInstances(project, zone, managedInstanceGroupName, requestBody);

      Operation response = request.execute();
      Operation.Error err = waitForOperationEnd(computeService, response);
      if (err == null || err.isEmpty()) {
        List<String> after = null;
        // as the waitForOperationEnd only waits for the operation to be scheduled
        // this loop waits until the requested machines actually go down (or up to a
        // certain amount of retries in checking)
        for (int i = 0; i < RUNNING_INSTANCES_MAX_RETRIES; i++) {
          after = getRunningInstances();
          if (after.size() == (before.size() - ids.size())) {
            break;
          }
          log.info("Machines not down yet, waiting");
          Thread.sleep(POLL_INTERVAL_MS);
        }
        before.removeAll(after); // keep only the ones no more present
        return new AutoScalingData(before);
      } else {
        log.error("Unable to terminate instances: %s", err.toPrettyString());
      }
    }
    catch (Exception e) {
      log.error(e, "Unable to terminate any instances.");
    }

    return new AutoScalingData(new ArrayList<>());
  }

  // Returns the list of the IDs of the machines running in the MIG
  private List<String> getRunningInstances()
  {
    final long maxResults = 500L; // 500 is sadly the max, see below

    ArrayList<String> ids = new ArrayList<>();
    try {
      final String project = envConfig.getProjectId();
      final String zone = envConfig.getZoneName();
      final String managedInstanceGroupName = envConfig.getManagedInstanceGroupName();

      Compute computeService = createComputeService();
      Compute.InstanceGroupManagers.ListManagedInstances request =
              computeService
                      .instanceGroupManagers()
                      .listManagedInstances(project, zone, managedInstanceGroupName);
      // Notice that while the doc says otherwise, there is not nextPageToken to page
      // through results and so everything needs to be in the same page
      request.setMaxResults(maxResults);
      InstanceGroupManagersListManagedInstancesResponse response = request.execute();
      for (ManagedInstance mi : response.getManagedInstances()) {
        ids.add(GceUtils.extractNameFromInstance(mi.getInstance()));
      }
      log.debug("Found running instances [%s]", String.join(",", ids));
    }
    catch (Exception e) {
      log.error(e, "Unable to get instances.");
    }
    return ids;
  }

  /**
   * Converts the IPs to IDs
   */
  @Override
  public List<String> ipToIdLookup(List<String> ips)
  {
    log.info("Asked IPs -> IDs for: [%s]", String.join(",", ips));

    if (ips.isEmpty()) {
      return new ArrayList<>();
    }

    // If the first one is not an IP, just assume all the other ones are not as well and just
    // return them as they are. This check is here because Druid does not check if IPs are
    // actually IPs and can send IDs to this function instead
    if (!InetAddresses.isInetAddress(ips.get(0))) {
      log.debug("Not IPs, doing nothing");
      return ips;
    }

    final String project = envConfig.getProjectId();
    final String zone = envConfig.getZoneName();
    try {
      Compute computeService = createComputeService();
      Compute.Instances.List request = computeService.instances().list(project, zone);
      // Cannot filter by IP atm, see below
      // request.setFilter(GceUtils.buildFilter(ips, "networkInterfaces[0].networkIP"));

      List<String> instanceIds = new ArrayList<>();
      InstanceList response;
      do {
        response = request.execute();
        if (response.getItems() == null) {
          continue;
        }
        for (Instance instance : response.getItems()) {
          // This stupid look up is needed because atm it is not possible to filter
          // by IP, see https://issuetracker.google.com/issues/73455339
          for (NetworkInterface ni : instance.getNetworkInterfaces()) {
            if (ips.contains(ni.getNetworkIP())) {
              instanceIds.add(instance.getName());
            }
          }
        }
        request.setPageToken(response.getNextPageToken());
      } while (response.getNextPageToken() != null);

      log.debug("Converted to [%s]", String.join(",", instanceIds));
      return instanceIds;
    }
    catch (Exception e) {
      log.error(e, "Unable to convert IPs to IDs.");
    }

    return new ArrayList<>();
  }

  /**
   * Converts the IDs to IPs - this is actually never called from the outside but it is called once
   * from inside the class if terminate is used instead of terminateWithIds
   */
  @Override
  public List<String> idToIpLookup(List<String> nodeIds)
  {
    log.info("Asked IDs -> IPs for: [%s]", String.join(",", nodeIds));

    if (nodeIds.isEmpty()) {
      return new ArrayList<>();
    }

    final String project = envConfig.getProjectId();
    final String zone = envConfig.getZoneName();

    try {
      Compute computeService = createComputeService();
      Compute.Instances.List request = computeService.instances().list(project, zone);
      request.setFilter(GceUtils.buildFilter(nodeIds, "name"));

      List<String> instanceIps = new ArrayList<>();
      InstanceList response;
      do {
        response = request.execute();
        if (response.getItems() == null) {
          continue;
        }
        for (Instance instance : response.getItems()) {
          // Assuming that every server has at least one network interface...
          String ip = instance.getNetworkInterfaces().get(0).getNetworkIP();
          // ...even though some IPs are reported as null on the spot but later they are ok,
          // so we skip the ones that are null. fear not, they are picked up later this just
          // prevents to have a machine called 'null' around which makes the caller wait for
          // it for maxScalingDuration time before doing anything else
          if (ip != null && !"null".equals(ip)) {
            instanceIps.add(ip);
          } else {
            // log and skip it
            log.warn("Call returned null IP for %s, skipping", instance.getName());
          }
        }
        request.setPageToken(response.getNextPageToken());
      } while (response.getNextPageToken() != null);

      return instanceIps;
    }
    catch (Exception e) {
      log.error(e, "Unable to convert IDs to IPs.");
    }

    return new ArrayList<>();
  }

  @Override
  public String toString()
  {
    return "gceAutoScaler={" +
        "envConfig=" + envConfig +
        ", maxNumWorkers=" + maxNumWorkers +
        ", minNumWorkers=" + minNumWorkers +
        '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GceAutoScaler that = (GceAutoScaler) o;

    return Objects.equals(envConfig, that.envConfig) &&
            minNumWorkers == that.minNumWorkers &&
            maxNumWorkers == that.maxNumWorkers;
  }

  @Override
  public int hashCode()
  {
    int result = 0;
    result = 31 * result + Objects.hashCode(envConfig);
    result = 31 * result + minNumWorkers;
    result = 31 * result + maxNumWorkers;
    return result;
  }
}
