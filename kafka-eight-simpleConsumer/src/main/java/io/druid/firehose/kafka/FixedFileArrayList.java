package io.druid.firehose.kafka;

import java.io.File;
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

    // implementation of remaining add methods....
}
