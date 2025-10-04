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
import db.engine.catalog.TableSchema;

public class StorageManager {
    private CatalogManager catalog;

    public StorageManager(CatalogManager catalog) {
        this.catalog = catalog;
    }

    // Inserts a record into the table file
    public void insertRecord(String tableName, Record record) {
        TableSchema tSchema = catalog.getTableSchema(tableName);
        if (tSchema == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        List<ColumnSchema> columns = tSchema.columns();
        validateRecord(columns, record);
        byte[] data = record.toBytes(columns);

    try (FileOutputStream fos = new FileOutputStream(tSchema.filePath(), true)) {
            // write record length
            fos.write(intToBytes(data.length));
            // write record bytes
            fos.write(data);
        } catch (IOException e) {
            e.printStackTrace();
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
                int recordLen = bytesToInt(bufLen);
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

    private byte[] intToBytes(int val) {
        return ByteBuffer.allocate(4).putInt(val).array();
    }

    private int bytesToInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
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
                case "INT" -> {
                    if (!(v instanceof Integer)) {
                        throw typeError(col, v);
                    }
                }
                case "BOOLEAN" -> {
                    if (!(v instanceof Boolean)) {
                        throw typeError(col, v);
                    }
                }
                case "VARCHAR" -> {
                    if (!(v instanceof String s)) {
                        throw typeError(col, v);
                    }
                    int max = col.length();
                    if (max > 0 && s.length() > max) {
                        throw new IllegalArgumentException("Value too long for column '" + col.name() + "' (max=" + max + ", got=" + s.length() + ")");
                    }
                }
                default -> throw new IllegalArgumentException("Unsupported column type: " + col.type());
            }
        }
    }

    private IllegalArgumentException typeError(ColumnSchema col, Object v) {
        return new IllegalArgumentException("Type mismatch for column '" + col.name() + "' expected " + col.type() + ", got " + (v == null ? "null" : v.getClass().getSimpleName()));
    }
}
