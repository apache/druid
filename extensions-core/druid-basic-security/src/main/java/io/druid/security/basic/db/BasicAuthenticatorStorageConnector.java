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

package io.druid.security.basic.db;

import java.util.List;
import java.util.Map;

public interface BasicAuthenticatorStorageConnector
{
  void createUser(String dbPrefix, String userName);

  void deleteUser(String dbPrefix, String userName);

  List<Map<String, Object>> getAllUsers(String dbPrefix);

  Map<String, Object> getUser(String dbPrefix, String userName);

  void setUserCredentials(String dbPrefix, String userName, char[] password);

  boolean checkCredentials(String dbPrefix, String userName, char[] password);

  Map<String, Object> getUserCredentials(String dbPrefix, String userName);

  void createUserTable(String dbPrefix);

  void createUserCredentialsTable(String dbPrefix);
}
