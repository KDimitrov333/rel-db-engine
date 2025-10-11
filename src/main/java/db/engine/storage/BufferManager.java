package db.engine.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Very small Last Recently Used (LRU) buffer manager for fixed-size pages.
 */
public class BufferManager {
    private final int pageSize; // size of individual pages; we set it to 4096 bytes by default
    private final int capacity; // max cached pages

    private final Map<PageKey, Page> cache;

    public BufferManager(int pageSize, int capacity) {
        this.pageSize = pageSize;
        this.capacity = capacity;
        // LinkedHashMap with access-ordering to implement LRU cache
        this.cache = new LinkedHashMap<>(capacity, 0.75f, true) {
            @Override   // Enforce LRU: when size exceeds capacity, remove oldest unused page from cache
            protected boolean removeEldestEntry(Map.Entry<PageKey, Page> eldest) {
                return size() > BufferManager.this.capacity;
            }
        };
    }

    public int getPageSize() { return pageSize; }

    /**
     * Obtain a page for a given file + pageId, loading it if absent.
     */
    public synchronized Page getPage(String filePath, int pageId) throws IOException {
        PageKey key = new PageKey(filePath, pageId);
        Page p = cache.get(key);
        if (p != null) return p;
        // Load from disk
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            long offset = (long) pageId * pageSize;
            if (offset >= raf.length()) {
                // Return an empty page (all zeros) if beyond EOF
                return cacheAndReturn(key, new byte[pageSize]);
            }
            raf.seek(offset);
            byte[] buf = new byte[pageSize];
            int read = raf.read(buf);
            if (read < pageSize && read > 0) {
                // Partial page at EOF; keep bytes read and leave remainder zeroed.
            }
            if (read == -1) {
                // No data; empty page
            }
            return cacheAndReturn(key, buf);
        }
    }

    /** Invalidate a page (e.g., after write). */
    public synchronized void invalidate(String filePath, int pageId) {
        cache.remove(new PageKey(filePath, pageId));
    }

    /** Invalidate a range of pages inclusive. */
    public synchronized void invalidateRange(String filePath, int startPageId, int endPageId) {
        for (int p = startPageId; p <= endPageId; p++) {
            cache.remove(new PageKey(filePath, p));
        }
    }

    private Page cacheAndReturn(PageKey key, byte[] buf) {
        Page p = new Page(key.filePath, key.pageId, buf);
        cache.put(key, p);
        return p;
    }

    private static final class PageKey {
        final String filePath;
        final int pageId;
        PageKey(String filePath, int pageId) {
            this.filePath = filePath;
            this.pageId = pageId;
        }
        @Override // so keys compare by logical identity (filePath + pageId)
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PageKey pk)) return false;
            return pageId == pk.pageId && filePath.equals(pk.filePath);
        }
        @Override // to align with equals(); filePath + pageId for hash map lookups
        public int hashCode() { return filePath.hashCode() * 31 + pageId; }
    }
}
