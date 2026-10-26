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

package org.apache.druid.testing;

public final class DruidCI
{
  private DruidCI()
  {
  }

  /**
   * Declares a Docker image used by tests so CI pulls it before the tests run.
   * Pass the image as a string literal: {@code .github/scripts/pull_docker_images} discovers images by
   * grepping these calls.
   *
   * @return the image, unchanged
   */
  public static String cacheDockerImage(final String image)
  {
    return image;
  }
}
