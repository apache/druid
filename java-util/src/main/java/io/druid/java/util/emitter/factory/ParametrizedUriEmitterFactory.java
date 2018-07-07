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

package io.druid.java.util.emitter.factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.emitter.core.Emitter;
import io.druid.java.util.emitter.core.ParametrizedUriEmitter;
import io.druid.java.util.emitter.core.ParametrizedUriEmitterConfig;
import org.asynchttpclient.AsyncHttpClient;

public class ParametrizedUriEmitterFactory extends ParametrizedUriEmitterConfig implements EmitterFactory
{

  @Override
  public Emitter makeEmitter(ObjectMapper objectMapper, AsyncHttpClient httpClient, Lifecycle lifecycle)
  {
    final Emitter retVal = new ParametrizedUriEmitter(this, httpClient, objectMapper);
    lifecycle.addManagedInstance(retVal);
    return retVal;
  }
}
