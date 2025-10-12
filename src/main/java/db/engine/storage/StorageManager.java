package db.engine.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;

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

    // Inserts a record into the table file - old method, does not expose RID
    public void insertRecord(String tableName, Record record) {
        insertRecordWithRid(tableName, record); // discard RID
    }

    /**
     * Inserts a record and returns its RID (file offset of the length prefix).
     */
    public RID insertRecordWithRid(String tableName, Record record) {
        TableSchema tSchema = catalog.getTableSchema(tableName);
        if (tSchema == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        List<ColumnSchema> columns = tSchema.columns();
        validateRecord(columns, record);
        byte[] data = record.toBytes(columns);

        File file = new File(tSchema.filePath());
        long offsetBeforeWrite = file.length(); // start of length prefix

        try (FileOutputStream fos = new FileOutputStream(file, true)) {
            fos.write(ByteBuffer.allocate(4).putInt(data.length).array());
            fos.write(data);
            if (indexManager != null) {
                indexManager.onTableInsert(tableName, new RID(offsetBeforeWrite), record);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null; // signal failure
        }
        // Invalidate affected pages in buffer cache
        long endOffset = offsetBeforeWrite + 4L + data.length;
        int startPage = (int) (offsetBeforeWrite / PAGE_SIZE);
        int endPage = (int) ((endOffset - 1) / PAGE_SIZE);
        bufferManager.invalidateRange(file.getPath(), startPage, endPage);
        return new RID(offsetBeforeWrite);
    }

    /**
     * Reads a single record given its RID.
     * @param tableName table to read from
     * @param rid record identifier returned by insertRecordWithRid
     * @return deserialized Record
     */
    public Record readRecordByRid(String tableName, RID rid) {
        TableSchema tSchema = catalog.getTableSchema(tableName);
        if (tSchema == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        List<ColumnSchema> columns = tSchema.columns();

        String path = tSchema.filePath();
        long offset = rid.offset();
        File file = new File(path);
        long fileLen = file.length();
        if (offset < 0 || offset >= fileLen) {
            throw new IllegalArgumentException("RID offset out of bounds: " + offset);
        }
        // Read length prefix (may cross a page boundary)
        byte[] lenBytes = readBytes(path, offset, 4);
        int len = ByteBuffer.wrap(lenBytes).getInt();
        if (len < 0) throw new RuntimeException("Negative record length at offset " + offset);
        byte[] recBytes = readBytes(path, offset + 4, len);
        return Record.fromBytes(recBytes, columns);
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

    public List<Record> scanTable(String tableName) {
        TableSchema tSchema = catalog.getTableSchema(tableName);
        if (tSchema == null) {
            throw new IllegalArgumentException("Table not found:" + tableName);
        }

        List<Record> results = new ArrayList<>();
        List<ColumnSchema> columns = tSchema.columns();

        long offset = 0L;
        try (FileInputStream fis = new FileInputStream(tSchema.filePath())) {
            byte[] bufLen = new byte[4];

            while (fis.read(bufLen) == 4) {
                offset += 4;
                int recordLen = ByteBuffer.wrap(bufLen).getInt();
                if (recordLen <= 0) {
                    System.err.println("[StorageManager] Invalid record length " + recordLen + " at offset " + (offset - 4));
                    break;
                }
                byte[] recordBuf = new byte[recordLen];
                int read = fis.read(recordBuf);
                if (read != recordLen) {
                    System.err.println("[StorageManager] Truncated record (expected=" + recordLen + ", got=" + read + ") at offset " + (offset - 4));
                    break;
                }
                offset += read;

                Record rec = Record.fromBytes(recordBuf, columns);
                results.add(rec);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return results;
    }

    /**
     * Full table scan returning each record with its RID
     */
    public List<TableRow> scanTableWithRids(String tableName) {
        TableSchema tSchema = catalog.getTableSchema(tableName);
        if (tSchema == null) {
            throw new IllegalArgumentException("Table not found:" + tableName);
        }
        List<TableRow> rows = new ArrayList<>();
        List<ColumnSchema> columns = tSchema.columns();
        long offset = 0L;
        try (FileInputStream fis = new FileInputStream(tSchema.filePath())) {
            byte[] lenBuf = new byte[4];
            while (true) {
                long recordStart = offset; // RID offset begins at length prefix
                int readLenBytes = fis.read(lenBuf);
                if (readLenBytes == -1) break; // EOF
                if (readLenBytes != 4) {
                    System.err.println("[StorageManager] Incomplete length field at offset " + recordStart);
                    break;
                }
                offset += 4;
                int recordLen = ByteBuffer.wrap(lenBuf).getInt();
                if (recordLen <= 0) {
                    System.err.println("[StorageManager] Invalid record length " + recordLen + " at offset " + recordStart);
                    break;
                }
                byte[] recordBuf = new byte[recordLen];
                int readData = fis.read(recordBuf);
                if (readData != recordLen) {
                    System.err.println("[StorageManager] Truncated record (expected=" + recordLen + ", got=" + readData + ") at offset " + recordStart);
                    break;
                }
                offset += readData;
                Record rec = Record.fromBytes(recordBuf, columns);
                rows.add(new TableRow(new RID(recordStart), rec));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rows;
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

    /**
     * Insert using heap page layout (PageRID).
     * Not yet supported.
     */
    public PageRID insertRecordHeap(String tableName, Record record) {
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

        return new PageRID(targetPageId, slotId);
    }

    /**
     * Read an arbitrary byte range using the buffer manager (spanning pages if needed)
     */
    private byte[] readBytes(String filePath, long offset, int length) {
        if (length < 0) throw new IllegalArgumentException("Negative length");
        byte[] out = new byte[length];
        int pageSize = bufferManager.getPageSize();
        int firstPage = (int) (offset / pageSize);
        int lastPage = (int) ((offset + length - 1) / pageSize);
        int destPos = 0;
        long currentOffset = offset;
        for (int pageId = firstPage; pageId <= lastPage; pageId++) {
            Page page;
            try {
                page = bufferManager.getPage(filePath, pageId);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load page " + pageId + " for file " + filePath, e);
            }
            long pageStart = (long) pageId * pageSize;
            int withinPageStart = (int) (currentOffset - pageStart);
            int bytesAvailable = pageSize - withinPageStart;
            int bytesNeeded = length - destPos;
            int toCopy = Math.min(bytesAvailable, bytesNeeded);
            System.arraycopy(page.data(), withinPageStart, out, destPos, toCopy);
            destPos += toCopy;
            currentOffset += toCopy;
        }
        return out;
    }
}
