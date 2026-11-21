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


Apache&circledR; Druid supports HTTP response compression and request decompression.

Druid compresses a response when the request's `Accept-Encoding` header lists an encoding that the server has enabled in `druid.server.http.compression.codecs`. If the client accepts more than one enabled encoding, the server uses the one listed first in `druid.server.http.compression.codecs`. Druid compresses responses to `GET` and `POST` requests.

Druid decompresses a request body when the request's `Content-Encoding` header is `gzip`, or `zstd` if `zstd` is in `druid.server.http.compression.codecs`, unless `druid.server.http.compression.gzip.inflateBufferSize` is 0.

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.http.compression.codecs`|Ordered list of response encodings the server may use, most preferred first. Supported values are `zstd` and `gzip`. Set to `["gzip"]` to use gzip only, as in earlier Druid versions. Set to `[]` to disable response compression.|`["zstd", "gzip"]`|
|`druid.server.http.compression.zstd.level`|zstd compression level, between 1 and 19. Higher levels produce smaller responses but use more CPU.|1|
|`druid.server.http.compression.gzip.level`|gzip compression level, between 0 and 9. Higher levels produce smaller responses but use more CPU. 0 means no compression. -1 means the zlib default, which is level 6.|-1 (level 6)|
|`druid.server.http.compressionLevel`|Deprecated. Use `druid.server.http.compression.gzip.level` instead. Applies only when `druid.server.http.compression.gzip.level` is not set.|Not set|
|`druid.server.http.compression.gzip.inflateBufferSize`|The buffer size used by the gzip request decoder. Set to 0 to disable request decompression for all encodings.|4096|
|`druid.server.http.inflateBufferSize`|Deprecated. Use `druid.server.http.compression.gzip.inflateBufferSize` instead. Applies only when `druid.server.http.compression.gzip.inflateBufferSize` is not set.|Not set|

Compression is negotiated for each request, so services that run different Druid versions can talk to each other during a rolling upgrade. Services that support zstd use it with each other, and use gzip with services that do not.

Druid's internal HTTP client sends `Accept-Encoding: zstd, gzip;q=0.5` by default, independent of `druid.server.http.compression.codecs`. To ask for a single encoding instead, set `druid.global.http.compressionCodec` or `druid.broker.http.compressionCodec` to `zstd`, `gzip`, or `identity`. Each service decodes `zstd`, `gzip`, and `deflate` responses.
