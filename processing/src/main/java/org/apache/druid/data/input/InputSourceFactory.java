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

package org.apache.druid.data.input;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.data.input.impl.LocalInputSourceFactory;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.guice.annotations.UnstableApi;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * An interface to generate a {@link SplittableInputSource} objects on the fly.
 * For composing input sources such as IcebergInputSource, the delegate input source instantiation might fail upon deserialization since the input file paths
 * are not available yet and this might fail the input source precondition checks.
 * This factory helps create the delegate input source once the input file paths are fully determined.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "local", value = LocalInputSourceFactory.class)
})
@UnstableApi
public interface InputSourceFactory
{
  SplittableInputSource create(List<String> inputFilePaths);

  /**
   * Creates a SplittableInputSource with optional credentials.
   *
   * <p>This method is used when reading data files from a catalog (like Iceberg REST catalog)
   * that vends temporary credentials for accessing the underlying storage. The credentials
   * map contains storage-specific keys that the implementing factory should interpret.
   *
   * <p>Common credential keys by storage type:
   * <ul>
   *   <li>S3: {@code s3.access-key-id}, {@code s3.secret-access-key}, {@code s3.session-token}</li>
   *   <li>GCS: {@code gcs.oauth2.token}, {@code gcs.oauth2.token-expires-at}</li>
   *   <li>Azure: {@code adls.sas-token.*}, {@code adls.connection-string.*}</li>
   * </ul>
   *
   * @param inputFilePaths list of file paths to read
   * @param vendedCredentials map of credential properties from the catalog, or null if no credentials vended
   * @return a SplittableInputSource configured with the credentials if provided
   */
  default SplittableInputSource create(List<String> inputFilePaths, @Nullable Map<String, String> vendedCredentials)
  {
    return create(inputFilePaths);
  }
}
