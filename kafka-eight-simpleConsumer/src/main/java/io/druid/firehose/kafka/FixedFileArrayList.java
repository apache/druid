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

package io.druid.firehose.kafka;

import java.io.File;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import com.metamx.common.logger.Logger;
public class FixedFileArrayList<T> extends ArrayList<T> {
	private static final Logger log = new Logger(
			FixedFileArrayList.class);
    private final int maxSize;
    public FixedFileArrayList(int maxSize) {
        super();
        this.maxSize = maxSize;
    }

    public boolean add(T t) {
        if (size() >= maxSize) {
        	T t0 = get(0);
        	File f = ((File) t0);
        	f.delete();
            remove(0);
        }
        return super.add(t);
    }

}
