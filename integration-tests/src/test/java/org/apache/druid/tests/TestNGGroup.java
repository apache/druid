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

package org.apache.druid.tests;

/**
 * These groups are used by continuous integration to split the integration tests into multiple jobs. Any tests that
 * are not annotated with a group will still be run by an "other" integration test continuous integration job.
 */
public class TestNGGroup
{
  public static final String BATCH_INDEX = "batch-index";
  public static final String HADOOP_INDEX = "hadoop-index";
  public static final String KAFKA_INDEX = "kafka-index";
  public static final String OTHER_INDEX = "other-index";
  public static final String QUERY = "query";
  public static final String REALTIME_INDEX = "realtime-index";
  public static final String SECURITY = "security";
}
