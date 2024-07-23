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

import io.fabric8.kubernetes.api.model.PodTemplate;

import javax.annotation.Nonnull;
import java.util.Objects;

public class PodTemplateWithName
{
  private final String name;
  private final PodTemplate podTemplate;

  public PodTemplateWithName(String name, PodTemplate podTemplate)
  {
    this.name = name;
    this.podTemplate = podTemplate;
  }

  @Nonnull
  public String getName()
  {
    return name;
  }

  @Nonnull
  public PodTemplate getPodTemplate()
  {
    return podTemplate;
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
    PodTemplateWithName that = (PodTemplateWithName) o;
    return Objects.equals(name, that.name) &&
        Objects.equals(podTemplate, that.podTemplate);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, podTemplate);
  }

  @Override
  public String toString()
  {
    return "PodTemplateWithName{" +
        "name='" + name + '\'' +
        ", podTemplate=" + podTemplate +
        '}';
  }
}
