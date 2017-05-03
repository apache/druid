/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.listener.resource;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.ws.rs.core.Response;
import java.io.InputStream;

/**
 * A handler for events related to the listening-announcer.
 * Developers are *STRONGLY* encouraged to use AbstractListenerHandler instead to adhere to return codes.
 */
public interface ListenerHandler
{
  Response handlePOST(InputStream inputStream, ObjectMapper mapper, String id);
  Response handlePOSTAll(InputStream inputStream, ObjectMapper mapper);
  Response handleGET(String id);
  Response handleGETAll();
  Response handleDELETE(String id);
  Response handleUpdates(InputStream inputStream, ObjectMapper mapper);

  void use_AbstractListenerHandler_instead();


}
