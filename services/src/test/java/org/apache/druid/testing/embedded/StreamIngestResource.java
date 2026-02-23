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

package org.apache.druid.testing.embedded;

import org.testcontainers.containers.GenericContainer;

import java.util.List;
import java.util.Map;

/**
 * Wrapper over a {@link TestcontainerResource} that can be used for testing
 * streaming ingestion. This is used as a fixture in embedded tests to allow
 * reuse of the same test methods for various types of streaming ingestion, e.g,
 * Kafka, Kinesis, etc.
 */
public abstract class StreamIngestResource<C extends GenericContainer<C>> extends TestcontainerResource<C>
{
  public abstract void createStreamWithPartitions(String stream, int partitionCount);

  public abstract void publishRecordsToStream(String stream, List<byte[]> records);

  public abstract void publishRecordsToStream(String stream, List<byte[]> records, Map<String, Object> properties);
}
