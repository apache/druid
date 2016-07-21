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

package io.druid.storage.oss;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.OSSObject;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.metamx.common.*;
import com.metamx.common.logger.Logger;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.segment.loading.URIDataPuller;
import io.druid.timeline.DataSegment;

import javax.tools.FileObject;
import java.io.*;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * A data segment puller that also hanldes URI data pulls.
 */
public class OssDataSegmentPuller implements DataSegmentPuller, URIDataPuller {

    public static final int DEFAULT_RETRY_COUNT = 3;

    public static FileObject buildFileObject(final URI uri, final OSSClient ossClient) {

        final OssCoords ossCoords = new OssCoords(uri);
        final OSSObject ossObject = ossClient.getObject(ossCoords.bucket, ossCoords.key);
        final String path = uri.getPath();

        return new FileObject() {

            final Object inputStreamOpener = new Object();
            volatile boolean streamAcquired = false;
            volatile OSSObject storageObject = ossObject;

            @Override
            public URI toUri() {
                return uri;
            }

            @Override
            public String getName() {
                final String ext = Files.getFileExtension(path);
                return Files.getNameWithoutExtension(path) + (Strings.isNullOrEmpty(ext) ? "" : ("." + ext));
            }

            @Override
            public InputStream openInputStream() throws IOException {

                synchronized (inputStreamOpener) {
                    if (streamAcquired) {
                        return storageObject.getObjectContent();
                    }

                    // lazily promote to full GET
                    storageObject = ossClient.getObject(storageObject.getBucketName(), storageObject.getKey());
                    final InputStream stream = storageObject.getObjectContent();
                    streamAcquired = true;
                    return stream;
                }
            }

            @Override
            public OutputStream openOutputStream() throws IOException {
                throw new UOE("Cannot stream Oss output");
            }

            @Override
            public Reader openReader(boolean ignoreEncodingErrors) throws IOException {
                throw new UOE("Cannot open reader");
            }

            @Override
            public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
                throw new UOE("Cannot open character sequence");
            }

            @Override
            public Writer openWriter() throws IOException {
                throw new UOE("Cannot open writer");
            }

            @Override
            public long getLastModified() {
                return ossObject.getObjectMetadata().getLastModified().getTime();
            }

            @Override
            public boolean delete() {
                throw new UOE("Cannot delete Oss items anonymously. jetS3t doesn't support authenticated deletes easily.");
            }
        };
    }

    public static final String scheme = OssStorageDruidModule.SCHEME;

    private static final Logger log = new Logger(OssDataSegmentPuller.class);

    protected static final String BUCKET = "bucket";
    protected static final String KEY = "baseKey";

    protected final OSSClient ossClient;

    @Inject
    public OssDataSegmentPuller(OSSClient ossClient) {
        this.ossClient = ossClient;
    }

    @Override
    public void getSegmentFiles(final DataSegment segment, final File outDir) throws SegmentLoadingException {
        getSegmentFiles(new OssCoords(segment), outDir);
    }

    public FileUtils.FileCopyResult getSegmentFiles(final OssCoords ossCoords, final File outDir)
            throws SegmentLoadingException {

        log.info("Pulling index at key[%s] to outDir[%s]", ossCoords, outDir);

        if (!isObjectInBucket(ossCoords)) {
            throw new SegmentLoadingException("IndexFile[%s] does not exist.", ossCoords);
        }

        if (!outDir.exists()) {
            outDir.mkdirs();
        }

        if (!outDir.isDirectory()) {
            throw new ISE("outDir[%s] must be a directory.", outDir);
        }

        try {
            final URI uri = URI.create(String.format("oss://%s/%s", ossCoords.bucket, ossCoords.key));
            final ByteSource byteSource = new ByteSource() {
                @Override
                public InputStream openStream() throws IOException {
                    return buildFileObject(uri, ossClient).openInputStream();
                }
            };
            if (CompressionUtils.isZip(ossCoords.key)) {
                final FileUtils.FileCopyResult result = CompressionUtils.unzip(
                        byteSource,
                        outDir,
                        OssUtils.OssRETRY,
                        true
                );
                log.info("Loaded %d bytes from [%s] to [%s]", result.size(), ossCoords.toString(), outDir.getAbsolutePath());
                return result;
            }
            if (CompressionUtils.isGz(ossCoords.key)) {
                final String fname = Files.getNameWithoutExtension(uri.getPath());
                final File outFile = new File(outDir, fname);

                final FileUtils.FileCopyResult result = CompressionUtils.gunzip(byteSource, outFile, OssUtils.OssRETRY);
                log.info("Loaded %d bytes from [%s] to [%s]", result.size(), ossCoords.toString(), outFile.getAbsolutePath());
                return result;
            }
            throw new IAE("Do not know how to load file type at [%s]", uri.toString());
        } catch (Exception e) {
            try {
                org.apache.commons.io.FileUtils.deleteDirectory(outDir);
            } catch (IOException ioe) {
                log.warn(
                        ioe,
                        "Failed to remove output directory [%s] for segment pulled from [%s]",
                        outDir.getAbsolutePath(),
                        ossCoords.toString()
                );
            }
            throw new SegmentLoadingException(e, e.getMessage());
        }
    }


    @Override
    public InputStream getInputStream(URI uri) throws IOException {
        return buildFileObject(uri, ossClient).openInputStream();
    }

    @Override
    public Predicate<Throwable> shouldRetryPredicate() {
        // Yay! smart retries!
        return new Predicate<Throwable>() {
            @Override
            public boolean apply(Throwable e) {
                if (e == null) {
                    return false;
                }
                if (OssUtils.OssRETRY.apply(e)) {
                    return true;
                }
                // Look all the way down the cause chain, just in case something wraps it deep.
                return apply(e.getCause());
            }
        };
    }

    /**
     * Returns the "version" (aka last modified timestamp) of the URI
     *
     * @param uri The URI to check the last timestamp
     *
     * @return The time in ms of the last modification of the URI in String format
     *
     * @throws IOException
     */
    @Override
    public String getVersion(URI uri) throws IOException {
        final FileObject object = buildFileObject(uri, ossClient);
        return String.format("%d", object.getLastModified());
    }

    private String toFilename(String key, final String suffix) {
        String filename = key.substring(key.lastIndexOf("/") + 1); // characters after last '/'
        filename = filename.substring(0, filename.length() - suffix.length()); // remove the suffix from the end
        return filename;
    }

    private boolean isObjectInBucket(final OssCoords coords) throws SegmentLoadingException {
        try {
            return OssUtils.retryOssOperation(
                    new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws Exception {
                            return ossClient.doesObjectExist(coords.bucket, coords.key);
                        }
                    }
            );
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    protected static class OssCoords {
        String bucket;
        String key;

        public OssCoords(URI uri) {
            if (!"oss".equalsIgnoreCase(uri.getScheme())) {
                throw new IAE("Unsupported scheme: [%s]", uri.getScheme());
            }
            bucket = uri.getHost();
            String path = uri.getPath();
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
            this.key = path;
        }

        public OssCoords(DataSegment segment) {
            Map<String, Object> loadSpec = segment.getLoadSpec();
            bucket = MapUtils.getString(loadSpec, BUCKET);
            key = MapUtils.getString(loadSpec, KEY);
            if (key.startsWith("/")) {
                key = key.substring(1);
            }
        }

        public OssCoords(String bucket, String key) {
            this.bucket = bucket;
            this.key = key;
        }

        public String toString() {
            return String.format("oss://%s/%s", bucket, key);
        }
    }
}
