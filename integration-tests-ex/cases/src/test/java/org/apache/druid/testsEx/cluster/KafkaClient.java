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

package org.apache.druid.testsEx.cluster;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.testsEx.config.ResolvedService.ResolvedKafka;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Simple Kafka client for a test cluster.
 * <p>
 * At present, this client contains just enough functionality to validate
 * that Kafka is up. It can be expanded to gather Kafka code currently
 * copy/pasted in tests.
 */
public class KafkaClient
{
  private final ResolvedKafka config;
  private Admin admin;

  public KafkaClient(ResolvedKafka config)
  {
    this.config = config;
  }

  public void open()
  {
    validate();
  }

  public Admin adminClient()
  {
    if (admin == null) {
      final Map<String, Object> props = new HashMap<>();
      props.put("bootstrap.servers", config.bootstrap());
      admin = Admin.create(props);
    }
    return admin;
  }

  public void validate()
  {
    DescribeClusterResult result = adminClient().describeCluster();
    try {
      if (result.nodes().get().isEmpty()) {
        throw new ISE("No nodes found in Kafka cluster");
      }
    }
    catch (InterruptedException | ExecutionException e) {
      throw new ISE(e, "Could not connect to Kafka");
    }
  }

  public void close()
  {
    if (admin != null) {
      admin.close();
      admin = null;
    }
  }
}
