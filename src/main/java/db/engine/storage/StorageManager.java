package db.engine.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

        List<ColumnSchema> columns = tSchema.getColumns();
        byte[] data = record.toBytes(columns);

        try (FileOutputStream fos = new FileOutputStream(tSchema.getFilePath(), true)) {
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
            File f = new File(schema.getFilePath());
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
        List<ColumnSchema> columns = tSchema.getColumns();

        try (FileInputStream fis = new FileInputStream(tSchema.getFilePath())) {
            byte[] bufLen = new byte[4];

            while (fis.read(bufLen) == 4) {
                int recordLen = bytesToInt(bufLen);
                byte[] recordBuf = new byte[recordLen];
                int read = fis.read(recordBuf);
                if (read != recordLen) break;

                Record rec = Record.fromBytes(recordBuf, columns);
                results.add(rec);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return results;
    }

    private byte[] intToBytes(int val) {
        return new byte[] {
            (byte)(val >>> 24),
            (byte)(val >>> 16),
            (byte)(val >>> 8),
            (byte) val
        };
    }

    private int bytesToInt(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) |
               ((bytes[1] & 0xFF) << 16) |
               ((bytes[2] & 0xFF) << 8)  |
               (bytes[3] & 0xFF);
    }
}
