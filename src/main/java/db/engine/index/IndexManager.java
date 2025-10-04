package db.engine.index;

import db.engine.catalog.CatalogManager;
import db.engine.catalog.IndexSchema;
import db.engine.catalog.TableSchema;
import db.engine.catalog.ColumnSchema;
import db.engine.storage.Record;
import db.engine.storage.StorageManager;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class IndexManager {
    private CatalogManager catalog;
    private StorageManager storage;
    private Map<String, BPlusTree> indexes;

    public IndexManager(CatalogManager catalog, StorageManager storage) {
        this.catalog = catalog;
        this.storage = storage;
        this.indexes = new HashMap<>();
    }

    // Create index for a table column (only INT supported for now)
    public void createIndex(String indexName, String tableName, String columnName) {
        TableSchema tSchema = catalog.getTableSchema(tableName);
        if (tSchema == null) throw new IllegalArgumentException("Table not found: " + tableName);

        List<ColumnSchema> cols = tSchema.columns();
        int colIndex = -1;
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).name().equals(columnName)) {
                colIndex = i;
                break;
            }
        }
        if (colIndex == -1) throw new IllegalArgumentException("Column not found: " + columnName);
        if (!cols.get(colIndex).type().equals("INT")) {
            throw new IllegalArgumentException("Indexing only supported on INT columns");
        }

        // Build in-memory B+ tree
        BPlusTree tree = new BPlusTree(4);
        List<Record> records = storage.scanTable(tableName);
        for (int rid = 0; rid < records.size(); rid++) {
            int key = (Integer) records.get(rid).getValues().get(colIndex);
            tree.insert(key, rid);
        }

        indexes.put(indexName, tree);

        // Register index in catalog
        IndexSchema iSchema = new IndexSchema(indexName, tableName, columnName, "indexes/" + indexName + ".idx");
        catalog.registerIndex(iSchema);

        try {
            File f = new File("indexes/" + indexName + ".idx");
            f.getParentFile().mkdirs();
            f.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // Lookup using an index
    public List<Record> lookup(String indexName, int key) {
        BPlusTree tree = indexes.get(indexName);
        if (tree == null) throw new IllegalArgumentException("Index not found: " + indexName);

        IndexSchema iSchema = catalog.getIndexSchema(indexName);
        List<Record> records = storage.scanTable(iSchema.table());

        List<Integer> rids = tree.search(key);
        List<Record> results = new ArrayList<>();
        for (int rid : rids) {
            results.add(records.get(rid));
        }
        return results;
    }
}
