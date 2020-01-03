---
id: http-compression
title: "HTTP compression"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->


Apache Druid supports http request decompression and response compression, to use this, http request header `Content-Encoding:gzip` and  `Accept-Encoding:gzip` is needed to be set.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.http.compressionLevel`|The compression level. Value should be between [-1,9], -1 for default level, 0 for no compression.|-1 (default compression level)|
|`druid.server.http.inflateBufferSize`|The buffer size used by gzip decoder. Set to 0 to disable request decompression.|4096|
