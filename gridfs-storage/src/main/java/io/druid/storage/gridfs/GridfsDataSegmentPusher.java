/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.storage.gridfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFSInputFile;
import io.druid.segment.SegmentUtils;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.timeline.DataSegment;
import io.druid.utils.CompressionUtils;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;

/**
 * GridFS Segment Pusher
 *
 * @author taity
 */
public class GridFSDataSegmentPusher extends GridFSStorage implements DataSegmentPusher {
    private static final Logger log = new Logger(GridFSDataSegmentPusher.class);
    private static final Joiner JOINER = Joiner.on("/").skipNulls();
    private final ObjectMapper jsonMapper;

    @Inject
    public GridFSDataSegmentPusher(GridFSDataSegmentConfig config, ObjectMapper jsonMapper) throws UnknownHostException {
        super(config);
        this.jsonMapper = jsonMapper;
    }

    @Override
    public String getPathForHadoop(String dataSource) {
        throw new UnsupportedOperationException("GridFS storage does not support indexing via Hadoop");
    }

    @Override
    public DataSegment push(final File indexFilesDir, DataSegment segment) throws IOException {
        log.info("Writing [%s] to C*", indexFilesDir);

        String key = JOINER.join(config.bucket.isEmpty() ? null : config.bucket, DataSegmentPusherUtil.getStorageDir(segment));

        // Create index
        final File compressedIndexFile = File.createTempFile("druid", "index.zip");
        long indexSize = CompressionUtils.zip(indexFilesDir, compressedIndexFile);
        log.info("Wrote compressed file [%s] to [%s]", compressedIndexFile.getAbsolutePath(), key);

        int version = SegmentUtils.getVersionFromDir(indexFilesDir);

        try {
            long start = System.currentTimeMillis();

            GridFSInputFile gsFile = this.gridFs.createFile(compressedIndexFile);

            byte[] json = jsonMapper.writeValueAsBytes(segment);
            DBObject metaData = new BasicDBObject();
            metaData.put("key", key);
            metaData.put("lastmodified", System.currentTimeMillis());
            metaData.put("descriptor", json);

            gsFile.setFilename(key);
            gsFile.setMetaData(metaData);
            gsFile.save();

            log.info("Wrote index to mongo in [%s] ms", System.currentTimeMillis() - start);
        } catch (Exception e) {
            throw new IOException(e);
        }

        segment = segment.withSize(indexSize).withLoadSpec(ImmutableMap.<String, Object>of("type", "mongo", "key", key)).withBinaryVersion(version);

        log.info("Deleting zipped index File[%s]", compressedIndexFile);
        compressedIndexFile.delete();
        return segment;
    }
}
