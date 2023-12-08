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

package org.apache.druid.k8s.overlord.taskadapter;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.druid.guice.FirehoseModule;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerConfig;
import org.apache.druid.k8s.overlord.common.DruidKubernetesClient;
import org.apache.druid.k8s.overlord.common.JobResponse;
import org.apache.druid.k8s.overlord.common.K8sTaskId;
import org.apache.druid.k8s.overlord.common.K8sTestUtils;
import org.apache.druid.k8s.overlord.common.KubernetesClientApi;
import org.apache.druid.k8s.overlord.common.KubernetesPeonClient;
import org.apache.druid.k8s.overlord.common.PeonCommandContext;
import org.apache.druid.k8s.overlord.common.PeonPhase;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

// must have a kind / minikube cluster installed and the image pushed to your repository
@Disabled
public class DruidPeonClientIntegrationTest
{
  private StartupLoggingConfig startupLoggingConfig;
  private TaskConfig taskConfig;
  private DruidNode druidNode;
  private KubernetesClientApi k8sClient;
  private KubernetesPeonClient peonClient;
  private ObjectMapper jsonMapper;

  @BeforeEach
  public void setup()
  {
    TestUtils utils = new TestUtils();
    jsonMapper = utils.getTestObjectMapper();
    for (Module jacksonModule : new FirehoseModule().getJacksonModules()) {
      jsonMapper.registerModule(jacksonModule);
    }
    jsonMapper.registerSubtypes(
        new NamedType(ParallelIndexTuningConfig.class, "index_parallel"),
        new NamedType(IndexTask.IndexTuningConfig.class, "index")
    );
    k8sClient = new DruidKubernetesClient();
    peonClient = new KubernetesPeonClient(k8sClient, "default", false, new NoopServiceEmitter());
    druidNode = new DruidNode(
        "test",
        null,
        false,
        null,
        null,
        true,
        false
    );
    startupLoggingConfig = new StartupLoggingConfig();
    taskConfig = new TaskConfigBuilder().setBaseDir("src/test/resources").build();
  }

  @Disabled
  @Test
  public void testDeployingSomethingToKind(@TempDir Path tempDir) throws Exception
  {
    PodSpec podSpec = K8sTestUtils.getDummyPodSpec();

    Task task = K8sTestUtils.getTask();
    KubernetesTaskRunnerConfig config = KubernetesTaskRunnerConfig.builder()
        .withNamespace("default")
        .build();
    K8sTaskAdapter adapter = new SingleContainerTaskAdapter(
        k8sClient,
        config,
        taskConfig,
        startupLoggingConfig,
        druidNode,
        jsonMapper,
        null
    );
    String taskBasePath = "/home/taskDir";
    PeonCommandContext context = new PeonCommandContext(Collections.singletonList(
        "sleep 10;  for i in `seq 1 1000`; do echo $i; done; exit 0"
    ), new ArrayList<>(), new File(taskBasePath));

    Job job = adapter.createJobFromPodSpec(podSpec, task, context);

    // launch the job and wait to start...
    peonClient.launchPeonJobAndWaitForStart(job, task, 1, TimeUnit.MINUTES);

    // there should be one job that is a k8s peon job that exists
    List<Job> jobs = peonClient.getPeonJobs();
    assertEquals(1, jobs.size());

    K8sTaskId taskId = new K8sTaskId(task.getId());
    InputStream peonLogs = peonClient.getPeonLogs(taskId).get();
    List<Integer> expectedLogs = IntStream.range(1, 1001).boxed().collect(Collectors.toList());
    List<Integer> actualLogs = new ArrayList<>();
    Thread thread = new Thread(() -> {
      try {
        actualLogs.addAll(IOUtils.readLines(peonLogs, "UTF-8")
                                 .stream()
                                 .map(Integer::parseInt)
                                 .collect(Collectors.toList()));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    thread.start();

    // assert that the env variable is corret
    Task taskFromEnvVar = adapter.toTask(job);
    assertEquals(task, taskFromEnvVar);

    // now copy the task.json file from the pod and make sure its the same as our task.json we expected
    Path downloadPath = Paths.get(tempDir.toAbsolutePath().toString(), "task.json");
    Pod mainJobPod = peonClient.getPeonPodWithRetries(taskId.getK8sJobName());
    k8sClient.executeRequest(client -> {
      client.pods()
            .inNamespace("default")
            .withName(mainJobPod.getMetadata().getName())
            .file(Paths.get(taskBasePath, "task.json").toString())
            .copy(downloadPath);
      return null;
    });

    String taskJsonFromPod = FileUtils.readFileToString(new File(downloadPath.toString()), StandardCharsets.UTF_8);
    Task taskFromPod = jsonMapper.readValue(taskJsonFromPod, Task.class);
    assertEquals(task, taskFromPod);


    JobResponse jobStatusResult = peonClient.waitForPeonJobCompletion(taskId, 2, TimeUnit.MINUTES);
    thread.join();
    assertEquals(PeonPhase.SUCCEEDED, jobStatusResult.getPhase());
    // as long as there were no exceptions we are good!
    assertEquals(expectedLogs, actualLogs);
    // cleanup my job
    assertTrue(peonClient.deletePeonJob(taskId));

    // we cleaned up the job, none should exist
    List<Job> existingJobs = peonClient.getPeonJobs();
    assertEquals(0, existingJobs.size());
  }
}
