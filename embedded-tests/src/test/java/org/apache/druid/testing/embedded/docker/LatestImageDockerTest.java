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

package org.apache.druid.testing.embedded.docker;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Interface for embedded tests which use {@link DruidContainerResource} running
 * the latest Druid image. All test classes implementing this interface should be
 * named {@code *DockerTest*} to allow maven to recognize them as integration tests,
 * and should use {@code EmbeddedHostname.containerFriendly()} to allow Druid
 * containers to be reachable by embedded servers and vice-versa.
 * <p>
 * DO NOT write a new Docker test unless absolutely necessary.
 * For all testing needs, use regular EmbeddedDruidServer-based tests only.
 * as they are much faster, easy to debug and do not require image downloads.
 */
@Tag("docker-test")
@EnabledIfSystemProperty(named = DruidContainerResource.PROPERTY_TEST_IMAGE, matches = ".+")
public interface LatestImageDockerTest
{

}
