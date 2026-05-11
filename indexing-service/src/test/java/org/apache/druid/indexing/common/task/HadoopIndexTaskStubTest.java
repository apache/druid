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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.segment.TestHelper;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class HadoopIndexTaskStubTest
{
  @Test
  void testSerde() throws JsonProcessingException
  {
    final String hadoop_json = """
        {
          "type" : "index_hadoop",
          "spec" : {
            "dataSchema" : {
              "dataSource" : "wikipedia",
              "parser" : {
                "type" : "hadoopyString",
                "parseSpec" : {
                  "format" : "json",
                  "dimensionsSpec" : {
                    "dimensions" : [
                      "channel",
                      "cityName",
                      "comment",
                      "countryIsoCode",
                      "countryName",
                      "isAnonymous",
                      "isMinor",
                      "isNew",
                      "isRobot",
                      "isUnpatrolled",
                      "metroCode",
                      "namespace",
                      "page",
                      "regionIsoCode",
                      "regionName",
                      "user",
                      { "name": "added", "type": "long" },
                      { "name": "deleted", "type": "long" },
                      { "name": "delta", "type": "long" }
                    ]
                  },
                  "timestampSpec" : {
                    "format" : "auto",
                    "column" : "time"
                  }
                }
              },
              "metricsSpec" : [],
              "granularitySpec" : {
                "type" : "uniform",
                "segmentGranularity" : "day",
                "queryGranularity" : "none",
                "intervals" : ["2015-09-12/2015-09-13"],
                "rollup" : false
              }
            },
            "ioConfig" : {
              "type" : "hadoop",
              "inputSpec" : {
                "type" : "static",
                "paths" : "/quickstart/wikiticker-2015-09-12-sampled.json.gz"
              }
            },
            "tuningConfig" : {
              "type" : "hadoop",
              "partitionsSpec" : {
                "type" : "hashed",
                "targetPartitionSize" : 5000000
              },
              "forceExtendableShardSpecs" : true,
              "jobProperties" : {
                "fs.default.name" : "hdfs://druid-hadoop-demo:9000",
                "fs.defaultFS" : "hdfs://druid-hadoop-demo:9000",
                "dfs.datanode.address" : "druid-hadoop-demo",
                "dfs.client.use.datanode.hostname" : "true",
                "dfs.datanode.use.datanode.hostname" : "true",
                "yarn.resourcemanager.hostname" : "druid-hadoop-demo",
                "yarn.nodemanager.vmem-check-enabled" : "false",
                "mapreduce.map.java.opts" : "-Duser.timezone=UTC -Dfile.encoding=UTF-8",
                "mapreduce.job.user.classpath.first" : "true",
                "mapreduce.reduce.java.opts" : "-Duser.timezone=UTC -Dfile.encoding=UTF-8",
                "mapreduce.map.memory.mb" : 1024,
                "mapreduce.reduce.memory.mb" : 1024,
                "yarn.app.mapreduce.am.command-opts" : "--add-opens=java.base/java.lang=ALL-UNNAMED",
                "mapreduce.job.classloader" : "true"
              }
            }
          },
          "hadoopDependencyCoordinates": ["org.apache.hadoop:hadoop-client-api:3.3.6","org.apache.hadoop:hadoop-client-runtime:3.3.6"]
        }
        """;

    ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    Task task = jsonMapper.readValue(hadoop_json, Task.class);
    Assertions.assertInstanceOf(HadoopIndexTaskStub.class, task);

    HadoopIndexTaskStub stub = (HadoopIndexTaskStub) task;
    Assertions.assertNotNull(stub.getSpec());
    Assertions.assertEquals(
        "wikipedia",
        ((Map<String, ?>) stub.getSpec().get("dataSchema")).get("dataSource")
    );

    HadoopIndexTaskStub again =
        (HadoopIndexTaskStub) jsonMapper.readValue(jsonMapper.writeValueAsString(task), Task.class);
    Assertions.assertEquals(stub.getSpec(), again.getSpec());
    Assertions.assertEquals(stub.getHadoopDependencyCoordinates(), again.getHadoopDependencyCoordinates());

    TaskToolbox toolBox = EasyMock.createNiceMock(TaskToolbox.class);
    Throwable t = Assertions.assertThrows(DruidException.class, () -> stub.runTask(toolBox));
    Assertions.assertEquals("The Apache Hadoop ingestion task was removed in Druid 37.0", t.getMessage());
  }
}
