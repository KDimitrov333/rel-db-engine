package db.engine.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
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

    public StorageManager(CatalogManager catalog) {
        this.catalog = catalog;
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
                indexManager.onTableInsert(tableName, record);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null; // signal failure
        }
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

        try (RandomAccessFile raf = new RandomAccessFile(tSchema.filePath(), "r")) {
            if (rid.offset() < 0 || rid.offset() >= raf.length()) {
                throw new IllegalArgumentException("RID offset out of bounds: " + rid.offset());
            }
            raf.seek(rid.offset());
            int len = raf.readInt();
            if (len < 0) throw new IOException("Negative record length at offset " + rid.offset());
            byte[] buf = new byte[len];
            raf.readFully(buf);
            return Record.fromBytes(buf, columns);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read record at RID=" + rid.offset() + " in table " + tableName, e);
        }
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
}
