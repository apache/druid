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

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.gridfs.GridFS;

import java.net.UnknownHostException;

/**
 * Superclass for accessing GridFS Storage.
 */
public class GridFSStorage {
    protected final GridFS gridFs;
    protected final GridFSDataSegmentConfig config;

    public GridFSStorage(GridFSDataSegmentConfig config) throws UnknownHostException {
        this.config = config;
        MongoClient mongo = new MongoClient(new MongoClientURI(config.uri));
        this.gridFs = new GridFS(mongo.getDB(config.db), config.bucket);
    }
}
