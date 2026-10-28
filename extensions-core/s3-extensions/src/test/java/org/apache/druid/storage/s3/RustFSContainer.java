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

package org.apache.druid.storage.s3;

import org.apache.druid.java.util.common.StringUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Testcontainer for <a href="https://github.com/rustfs/rustfs">RustFS</a>, an S3-compatible object store, for tests
 * that need a real S3 endpoint.
 */
public class RustFSContainer extends GenericContainer<RustFSContainer>
{
  public static final DockerImageName IMAGE = DockerImageName.parse("rustfs/rustfs:1.0.0");
  public static final String DEFAULT_ACCESS_KEY = "rustfsadmin";
  public static final String DEFAULT_SECRET_KEY = "rustfsadmin";

  private static final int S3_PORT = 9000;

  private String accessKey = DEFAULT_ACCESS_KEY;
  private String secretKey = DEFAULT_SECRET_KEY;

  public RustFSContainer()
  {
    super(IMAGE);
    withExposedPorts(S3_PORT);
    waitingFor(Wait.forHttp("/health/ready").forPort(S3_PORT));
  }

  public RustFSContainer withCredentials(String accessKey, String secretKey)
  {
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    return this;
  }

  @Override
  protected void configure()
  {
    withEnv("RUSTFS_ACCESS_KEY", accessKey);
    withEnv("RUSTFS_SECRET_KEY", secretKey);
  }

  public String getS3URL()
  {
    return StringUtils.format("http://%s:%d", getHost(), getMappedPort(S3_PORT));
  }

  public String getAccessKey()
  {
    return accessKey;
  }

  public String getSecretKey()
  {
    return secretKey;
  }
}
