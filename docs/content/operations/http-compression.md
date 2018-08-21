---
layout: doc_page
---
# HTTP Compression

Druid supports http request decompression and response compression, to use this, http request header `Content-Encoding:gzip` and  `Accept-Encoding:gzip` is needed to be set.

# General Configuration

|Property|Description|Default|
|--------|-----------|-------|
|`druid.server.http.compressionLevel`|The compression level. Value should be between [-1,9], -1 for default level, 0 for no compression.|-1 (default compression level)|
|`druid.server.http.inflateBufferSize`|The buffer size used by gzip decoder. Set to 0 to disable request decompression.|4096|