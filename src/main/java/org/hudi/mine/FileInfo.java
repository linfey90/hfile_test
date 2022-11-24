package org.hudi.mine;

import org.hudi.bloom.BloomFilter;
import org.hudi.storage.HoodieHFileReader;

public class FileInfo {
    private String minKey;
    private String maxKey;
    private HoodieHFileReader hoodieHFileReader;
    private BloomFilter filter;

    public String getMinKey() {
        return minKey;
    }

    public void setMinKey(String minKey) {
        this.minKey = minKey;
    }

    public String getMaxKey() {
        return maxKey;
    }

    public void setMaxKey(String maxKey) {
        this.maxKey = maxKey;
    }

    public HoodieHFileReader getHoodieHFileReader() {
        return hoodieHFileReader;
    }

    public void setHoodieHFileReader(HoodieHFileReader hoodieHFileReader) {
        this.hoodieHFileReader = hoodieHFileReader;
    }

    public BloomFilter getFilter() {
        return filter;
    }

    public void setFilter(BloomFilter filter) {
        this.filter = filter;
    }
}
