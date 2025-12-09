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

package org.apache.druid.k8s.overlord.common;

import io.fabric8.kubernetes.client.informers.ResourceEventHandler;

import java.util.function.BiConsumer;

/**
 * Implementation of ResourceEventHandler that simplifies event handling
 * by accepting a single lambda BiConsumer for all event types (add, update, delete).
 *
 * @param <T> The Kubernetes resource type (e.g., Pod, Job)
 */
public class InformerEventHandler<T> implements ResourceEventHandler<T>
{
  private final BiConsumer<T, InformerEventType> eventConsumer;

  public InformerEventHandler(BiConsumer<T, InformerEventType> eventConsumer)
  {
    this.eventConsumer = eventConsumer;
  }

  @Override
  public void onAdd(T resource)
  {
    eventConsumer.accept(resource, InformerEventType.ADD);
  }

  @Override
  public void onUpdate(T oldResource, T newResource)
  {
    eventConsumer.accept(newResource, InformerEventType.UPDATE);
  }

  @Override
  public void onDelete(T resource, boolean deletedFinalStateUnknown)
  {
    eventConsumer.accept(resource, InformerEventType.DELETE);
  }
}
