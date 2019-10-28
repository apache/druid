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

package org.apache.druid.indexer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class Utils
{
  private static final Logger log = new Logger(Utils.class);
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  public static OutputStream makePathAndOutputStream(JobContext job, Path outputPath, boolean deleteExisting)
      throws IOException
  {
    OutputStream retVal;
    FileSystem fs = outputPath.getFileSystem(job.getConfiguration());
    Class<? extends CompressionCodec> codecClass;
    CompressionCodec codec = null;

    if (FileOutputFormat.getCompressOutput(job)) {
      codecClass = FileOutputFormat.getOutputCompressorClass(job, GzipCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, job.getConfiguration());
      outputPath = new Path(outputPath + codec.getDefaultExtension());
    }

    if (fs.exists(outputPath)) {
      if (deleteExisting) {
        fs.delete(outputPath, false);
      } else {
        throw new ISE("outputPath[%s] must not exist.", outputPath);
      }
    }

    if (FileOutputFormat.getCompressOutput(job)) {
      retVal = codec.createOutputStream(fs.create(outputPath, false));
    } else {
      retVal = fs.create(outputPath, false);
    }
    return retVal;
  }

  public static InputStream openInputStream(JobContext job, Path inputPath) throws IOException
  {
    return openInputStream(job, inputPath, inputPath.getFileSystem(job.getConfiguration()));
  }

  public static boolean exists(JobContext job, FileSystem fs, Path inputPath) throws IOException
  {
    if (!FileOutputFormat.getCompressOutput(job)) {
      return fs.exists(inputPath);
    } else {
      Class<? extends CompressionCodec> codecClass = FileOutputFormat.getOutputCompressorClass(job, GzipCodec.class);
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job.getConfiguration());
      return fs.exists(new Path(inputPath + codec.getDefaultExtension()));
    }
  }

  public static InputStream openInputStream(JobContext job, Path inputPath, final FileSystem fileSystem)
      throws IOException
  {
    if (!FileOutputFormat.getCompressOutput(job)) {
      return fileSystem.open(inputPath);
    } else {
      Class<? extends CompressionCodec> codecClass = FileOutputFormat.getOutputCompressorClass(job, GzipCodec.class);
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job.getConfiguration());
      inputPath = new Path(inputPath + codec.getDefaultExtension());

      return codec.createInputStream(fileSystem.open(inputPath));
    }
  }

  public static Map<String, Object> getStats(JobContext job, Path statsPath)
      throws IOException
  {
    FileSystem fs = statsPath.getFileSystem(job.getConfiguration());

    return JSON_MAPPER.readValue(
        fs.open(statsPath),
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );
  }

  public static void storeStats(JobContext job, Path path, Map<String, Object> stats) throws IOException
  {
    JSON_MAPPER.writeValue(makePathAndOutputStream(job, path, true), stats);
  }

  @Nullable
  public static String getFailureMessage(Job failedJob, ObjectMapper jsonMapper)
  {
    try {
      Map<String, String> taskDiagsMap = new HashMap<>();
      TaskCompletionEvent[] completionEvents = failedJob.getTaskCompletionEvents(0, 100);
      for (TaskCompletionEvent tce : completionEvents) {
        String[] taskDiags = failedJob.getTaskDiagnostics(tce.getTaskAttemptId());
        String combinedTaskDiags = "";
        for (String taskDiag : taskDiags) {
          combinedTaskDiags += taskDiag;
        }
        taskDiagsMap.put(tce.getTaskAttemptId().toString(), combinedTaskDiags);
      }
      return jsonMapper.writeValueAsString(taskDiagsMap);
    }
    catch (IOException | InterruptedException ie) {
      log.error(ie, "couldn't get failure cause for job [%s]", failedJob.getJobName());
      return null;
    }
  }

  /**
   * It is possible for a Hadoop Job to succeed, but for `job.waitForCompletion()` to fail because of
   * issues with the JobHistory server.
   *
   * When the JobHistory server is unavailable, it's possible to fetch the application's status
   * from the YARN ResourceManager instead.
   *
   * Returns true if both `useYarnRMJobStatusFallback` is enabled and YARN ResourceManager reported success for the
   * target job.
   */
  public static boolean checkAppSuccessForJobIOException(
      IOException ioe,
      Job job,
      boolean useYarnRMJobStatusFallback
  )
  {
    if (!useYarnRMJobStatusFallback) {
      log.info("useYarnRMJobStatusFallback is false, not checking YARN ResourceManager.");
      return false;
    }
    log.error(ioe, "Encountered IOException with job, checking application success from YARN ResourceManager.");

    boolean success = checkAppSuccessFromYarnRM(job);
    if (!success) {
      log.error("YARN RM did not report job success either.");
    }
    return success;
  }

  public static boolean checkAppSuccessFromYarnRM(Job job)
  {
    final HttpClient httpClient = new HttpClient();
    final AtomicBoolean succeeded = new AtomicBoolean(false);
    try {
      httpClient.start();
      RetryUtils.retry(
          () -> {
            checkAppSuccessFromYarnRMOnce(httpClient, job, succeeded);
            return null;
          },
          ex -> {
            return !succeeded.get();
          },
          5
      );
      return succeeded.get();
    }
    catch (Exception e) {
      log.error(e, "Got exception while trying to contact YARN RM.");
      // we're already in a best-effort fallback failure handling case, just stop if we have issues with the http client
      return false;
    }
    finally {
      try {
        httpClient.stop();
      }
      catch (Exception e) {
        log.error(e, "Got exception with httpClient.stop() while trying to contact YARN RM.");
      }
    }
  }

  private static void checkAppSuccessFromYarnRMOnce(
      HttpClient httpClient,
      Job job,
      AtomicBoolean succeeded
  ) throws IOException, InterruptedException, ExecutionException, TimeoutException
  {
    String appId = StringUtils.replace(job.getJobID().toString(), "job", "application");
    String yarnRM = job.getConfiguration().get("yarn.resourcemanager.webapp.address");
    String yarnEndpoint = StringUtils.format("http://%s/ws/v1/cluster/apps/%s", yarnRM, appId);
    log.info("Attempting to retrieve app status from YARN ResourceManager at [%s].", yarnEndpoint);

    ContentResponse res = httpClient.GET(yarnEndpoint);
    log.info("App status response from YARN RM: " + res.getContentAsString());
    Map<String, Object> respMap = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(
        res.getContentAsString(),
        new TypeReference<Map<String, Object>>()
        {
        }
    );

    Map<String, Object> appMap = (Map<String, Object>) respMap.get("app");
    String state = (String) appMap.get("state");
    String finalStatus = (String) appMap.get("finalStatus");
    if ("FINISHED".equals(state) && "SUCCEEDED".equals(finalStatus)) {
      succeeded.set(true);
    }
  }
}
