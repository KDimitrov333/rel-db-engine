package db.engine.storage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import db.engine.catalog.CatalogManager;
import db.engine.catalog.ColumnSchema;
import db.engine.index.IndexManager;
import db.engine.catalog.TableSchema;

public class StorageManager {
    private CatalogManager catalog;
    private IndexManager indexManager; // optional; may be set after construction
    private final BufferManager bufferManager;

    // Fixed page size for initial buffer manager introduction
    public static final int PAGE_SIZE = 4096;

    public StorageManager(CatalogManager catalog) {
        this.catalog = catalog;
        this.bufferManager = new BufferManager(PAGE_SIZE, 64); // capacity 64 pages
    }

    // Allow late binding to avoid circular construction concerns
    public void attachIndexManager(IndexManager indexManager) {
        this.indexManager = indexManager;
    }

    public void createTable(TableSchema schema) {
        catalog.registerTable(schema);

        try {
            File f = new File(schema.filePath());
            f.getParentFile().mkdirs();
            f.createNewFile();  // actual storage
            System.out.println("Created table file: " + f.getPath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public PageRID insert(String tableName, Record record) { return doHeapInsert(tableName, record); }

    public Record read(String tableName, PageRID rid) {
        TableSchema ts = catalog.getTableSchema(tableName);
        if (ts == null) throw new IllegalArgumentException("Table not found: " + tableName);
        List<ColumnSchema> cols = ts.columns();
        byte[] pageBytes;
        try {
            pageBytes = bufferManager.getPage(ts.filePath(), rid.pageId()).data();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        HeapPage hp = HeapPage.wrap(ts.filePath(), rid.pageId(), pageBytes, PAGE_SIZE);
        return hp.readRecord(rid.slotId(), cols);
    }

    /**
     * Delete (tombstone) a record identified by PageRID. Returns true if a record existed and
     * was marked deleted; false if slot was already tombstoned or out of range.
     */
    public boolean delete(String tableName, PageRID rid) {
        TableSchema ts = catalog.getTableSchema(tableName);
        if (ts == null) throw new IllegalArgumentException("Table not found: " + tableName);
        List<ColumnSchema> cols = ts.columns();
        byte[] pageBytes;
        try {
            pageBytes = bufferManager.getPage(ts.filePath(), rid.pageId()).data();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        HeapPage hp = HeapPage.wrap(ts.filePath(), rid.pageId(), pageBytes, PAGE_SIZE);
        // Try to read old record (will throw if tombstoned)
        Record old;
        try {
            old = hp.readRecord(rid.slotId(), cols);
        } catch (Exception ex) { return false; }
        hp.delete(rid.slotId());
        // Persist page after mutation
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(new File(ts.filePath()), "rw")) {
            raf.seek((long) rid.pageId() * PAGE_SIZE);
            raf.write(hp.rawData());
        } catch (IOException e) {
            throw new RuntimeException("Failed writing heap page on delete " + rid.pageId(), e);
        }
        bufferManager.invalidate(ts.filePath(), rid.pageId());
        if (indexManager != null) {
            indexManager.onTableDelete(tableName, rid, old);
        }
        return true;
    }

    // Functional-style scan using callback to avoid building large lists when not needed
    public interface RowConsumer { void accept(PageRID rid, Record record); }

    public void scan(String tableName, RowConsumer consumer) {
        TableSchema ts = catalog.getTableSchema(tableName);
        if (ts == null) throw new IllegalArgumentException("Table not found: " + tableName);
        File f = new File(ts.filePath());
        long fileLen = f.length();
        if (fileLen == 0) return;
        int pageCount = (int) ((fileLen + PAGE_SIZE - 1) / PAGE_SIZE);
        List<ColumnSchema> cols = ts.columns();
        for (int pid = 0; pid < pageCount; pid++) {
            byte[] bytes;
            try { bytes = bufferManager.getPage(ts.filePath(), pid).data(); } catch (IOException e) { throw new RuntimeException(e); }
            HeapPage hp = HeapPage.wrap(ts.filePath(), pid, bytes, PAGE_SIZE);
            for (int slotId : hp.liveSlotIds()) {
                consumer.accept(new PageRID(pid, slotId), hp.readRecord(slotId, cols));
            }
        }
    }

    public List<Record> scanTable(String tableName) {
        List<Record> out = new ArrayList<>();
        scan(tableName, (rid, rec) -> out.add(rec));
        return out;
    }

    // Validation: arity, type consistency, VARCHAR length constraint
    private void validateRecord(List<ColumnSchema> columns, Record record) {
        List<Object> vals = record.getValues();
        if (vals.size() != columns.size()) {
            throw new IllegalArgumentException("Arity mismatch: expected " + columns.size() + " values, got " + vals.size());
        }
        for (int i = 0; i < columns.size(); i++) {
            ColumnSchema col = columns.get(i);
            Object v = vals.get(i);
            switch (col.type()) {
                case INT -> {
                    if (!(v instanceof Integer)) {
                        throw typeError(col, v);
                    }
                }
                case BOOLEAN -> {
                    if (!(v instanceof Boolean)) {
                        throw typeError(col, v);
                    }
                }
                case VARCHAR -> {
                    if (!(v instanceof String s)) {
                        throw typeError(col, v);
                    }
                    int max = col.length();
                    if (max > 0) {
                        int byteLen = s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                        if (byteLen > max) {
                            throw new IllegalArgumentException("Value too long for column '" + col.name() + "' (max=" + max + " bytes, got=" + byteLen + ")");
                        }
                    }
                }
            }
        }
    }

    private IllegalArgumentException typeError(ColumnSchema col, Object v) {
        return new IllegalArgumentException("Type mismatch for column '" + col.name() + "' expected " + col.type() + ", got " + (v == null ? "null" : v.getClass().getSimpleName()));
    }

    private PageRID doHeapInsert(String tableName, Record record) {
        TableSchema tSchema = catalog.getTableSchema(tableName);
        if (tSchema == null) throw new IllegalArgumentException("Table not found: " + tableName);
        List<ColumnSchema> columns = tSchema.columns();
        validateRecord(columns, record);

        byte[] payload = record.toBytes(columns);
        if (payload.length > 0xFFFF) {
            throw new IllegalArgumentException("Record too large for current heap page format (len=" + payload.length + ")");
        }

        File file = new File(tSchema.filePath());
        try {
            file.getParentFile().mkdirs();
            file.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        long fileLen = file.length();
        int lastPageId = (int) (fileLen / PAGE_SIZE);
        boolean aligned = (fileLen % PAGE_SIZE) == 0; // true if no partial page at end
        int targetPageId = aligned ? Math.max(lastPageId - 1, 0) : lastPageId; // if empty file -> page 0

        byte[] pageBytes;
        try {
            pageBytes = bufferManager.getPage(file.getPath(), targetPageId).data();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load page " + targetPageId, e);
        }
        HeapPage heapPage = HeapPage.wrap(file.getPath(), targetPageId, pageBytes, PAGE_SIZE);

        if (!heapPage.canFit(payload.length)) {
            targetPageId = aligned ? lastPageId : lastPageId + 1; // append new page at end
            pageBytes = new byte[PAGE_SIZE];
            heapPage = HeapPage.wrap(file.getPath(), targetPageId, pageBytes, PAGE_SIZE);
        }

        int slotId = heapPage.insert(payload);

        // Persist page
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, "rw")) {
            raf.seek((long) targetPageId * PAGE_SIZE);
            raf.write(heapPage.rawData());
        } catch (IOException e) {
            throw new RuntimeException("Failed writing heap page " + targetPageId, e);
        }
        bufferManager.invalidate(file.getPath(), targetPageId);

        PageRID rid = new PageRID(targetPageId, slotId);
        if (indexManager != null) {
            indexManager.onTableInsert(tableName, rid, record);
        }
        return rid;
    }
}
