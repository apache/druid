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
import java.net.URISyntaxException;

/**
 * A data segment puller that also hanldes URI data pulls.
 */
public class OssDataSegmentPuller implements DataSegmentPuller, URIDataPuller {

    private static final Logger log = new Logger(OssDataSegmentPuller.class);

    protected static final String BUCKET = "bucket";
    protected static final String KEY = "key";
    protected final OSSClient ossClient;

    @Inject
    public OssDataSegmentPuller(OSSClient ossClient) {
        this.ossClient = ossClient;
    }

    @Override
    public void getSegmentFiles(final DataSegment segment, final File outDir) throws SegmentLoadingException {
        getSegmentFiles(getURI(segment), outDir);
    }

    @Override
    public InputStream getInputStream(URI uri) throws IOException {
        return buildFileObjects(uri, ossClient).openInputStream();
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

    @Override
    public String getVersion(URI uri) {
        return String.format("%d", buildFileObjects(uri, ossClient).getLastModified());
    }

    public FileUtils.FileCopyResult getSegmentFiles(final URI uri, final File outDir) throws SegmentLoadingException {

        //  get bucket and key from uri
        final String bucket = getBucket(uri);
        final String key = getKey(uri);

        //  get index from oss
        log.info("pulling index from oss key [%s] to out dir [%s]", key, outDir);
        if (!ossClient.doesObjectExist(bucket, key)) {
            throw new SegmentLoadingException("index file [%s] does not exist", key);
        }

        //  create out directory
        outDir.mkdirs();
        if (!outDir.isDirectory()) {
            throw new ISE("out dir [%s] must be a directory", outDir);
        }

        try {

            //  get index input stream
            final ByteSource byteSource = new ByteSource() {
                @Override
                public InputStream openStream() throws IOException {
                    return ossClient.getObject(bucket, key).getObjectContent();
                }
            };

            //  if zip file
            if (CompressionUtils.isZip(key)) {
                final FileUtils.FileCopyResult result = CompressionUtils.unzip(
                        byteSource,
                        outDir,
                        OssUtils.OssRETRY,
                        true
                );
                log.info("loaded %d bytes from oss [%s] to [%s]", result.size(), key, outDir.getAbsolutePath());
                return result;
            }

            //   if gz file
            if (CompressionUtils.isGz(key)) {
                final File outFile = new File(outDir, Files.getNameWithoutExtension(uri.getPath()));
                final FileUtils.FileCopyResult result = CompressionUtils.gunzip(
                        byteSource,
                        outFile,
                        OssUtils.OssRETRY
                );
                log.info("loaded %d bytes from oss [%s] to [%s]", result.size(), key, outFile.getAbsolutePath());
                return result;
            }

            throw new IAE("do not know how to load file type at [%s]", uri.toString());

        } catch (Exception e) {

            try {

                //  delete out directory
                org.apache.commons.io.FileUtils.deleteDirectory(outDir);

            } catch (IOException ioe) {
                log.warn(
                        ioe,
                        "failed to remove output directory [%s] for segment pulled from [%s]",
                        outDir.getAbsolutePath(),
                        uri.getPath()
                );
            }

            throw new SegmentLoadingException(e, e.getMessage());
        }
    }

    private FileObject buildFileObjects(final URI uri, final OSSClient ossClient) {

        //  get index data from oss
        final OSSObject ossObject = ossClient.getObject(getBucket(uri), getKey(uri));

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
                return Files.getNameWithoutExtension(uri.getPath()) + "." + Files.getFileExtension(uri.getPath());
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
                throw new UOE("cannot stream oss output");
            }

            @Override
            public Reader openReader(boolean ignoreEncodingErrors) throws IOException {
                throw new UOE("cannot open reader");
            }

            @Override
            public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
                throw new UOE("cannot open character sequence");
            }

            @Override
            public Writer openWriter() throws IOException {
                throw new UOE("cannot open writer");
            }

            @Override
            public long getLastModified() {
                return ossObject.getObjectMetadata().getLastModified().getTime();
            }

            @Override
            public boolean delete() {
                throw new UOE("cannot delete oss items anonymously");
            }
        };
    }

    private String getBucket(URI uri) {
        return uri.getHost();
    }

    private String getKey(URI uri) {
        return uri.getPath().substring(1);
    }

    private URI getURI(DataSegment segment) {
        try {
            return new URI("oss://" + MapUtils.getString(segment.getLoadSpec(), BUCKET) + MapUtils.getString(segment.getLoadSpec(), KEY));
        } catch (URISyntaxException e) {
            e.printStackTrace();
            return null;
        }
    }
}