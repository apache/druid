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

import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.utils.CompressionUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.net.UnknownHostException;

/**
 * GridFS Segment Puller
 *
 * @author taity
 */
public class GridFSDataSegmentPuller extends GridFSStorage implements DataSegmentPuller {
    private static final Logger log = new Logger(GridFSDataSegmentPuller.class);

    @Inject
    public GridFSDataSegmentPuller(GridFSDataSegmentConfig config) throws UnknownHostException {
        super(config);
    }

    @Override
    public void getSegmentFiles(DataSegment segment, File outDir) throws SegmentLoadingException {
        String key = (String) segment.getLoadSpec().get("key");
        log.info("Pulling index from mongo at path[%s] to outDir[%s]", key, outDir);

        if (!outDir.exists()) {
            outDir.mkdirs();
        }

        if (!outDir.isDirectory()) {
            throw new ISE("outDir[%s] must be a directory.", outDir);
        }

        long startTime = System.currentTimeMillis();
        final File outFile = new File(outDir, "index.zip");
        try {
            try {
                log.info("Writing to [%s]", outFile.getAbsolutePath());
                this.gridFs.findOne(key).writeTo(outFile);
                CompressionUtils.unzip(outFile, outDir);
            } catch (Exception e) {
                FileUtils.deleteDirectory(outDir);
            }
        } catch (Exception e) {
            throw new SegmentLoadingException(e, e.getMessage());
        }
        log.info("Pull of file[%s] completed in %,d millis", key, System.currentTimeMillis() - startTime);
    }

    @Override
    public long getLastModified(DataSegment segment) throws SegmentLoadingException {
        String key = (String) segment.getLoadSpec().get("key");
        long lastModified = Long.valueOf(this.gridFs.findOne(key).getMetaData().get("lastmodified").toString()).longValue();
        log.info("Read lastModified for [%s] as [%d]", key, lastModified);
        return lastModified;
    }
}
