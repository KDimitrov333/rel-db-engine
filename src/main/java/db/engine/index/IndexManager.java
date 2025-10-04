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
    private final Map<String, IndexState> indexStates;
    private final Map<String, Integer> tableRowCounts;

    public IndexManager(CatalogManager catalog, StorageManager storage) {
        this.catalog = catalog;
        this.storage = storage;
        this.indexStates = new HashMap<>();
        this.tableRowCounts = new HashMap<>();
        this.storage.attachIndexManager(this);
    }

    // Create index for a table column (only INT supported for now)
    public void createIndex(String indexName, String tableName, String columnName) {
        TableSchema tSchema = catalog.getTableSchema(tableName);
        if (tSchema == null) throw new IllegalArgumentException("Table not found: " + tableName);

        List<ColumnSchema> cols = tSchema.columns();
        int colIndex = findColumnIndex(cols, columnName);
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

        indexStates.put(indexName, new IndexState(indexName, tableName, columnName, colIndex, tree));

        // Initialize / update row count baseline (only set if absent to retain longest-lived counter)
        tableRowCounts.putIfAbsent(tableName, records.size());

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
        IndexState state = indexStates.get(indexName);
        if (state == null) throw new IllegalArgumentException("Index not found: " + indexName);

        IndexSchema iSchema = catalog.getIndexSchema(indexName);
        String table = (iSchema != null) ? iSchema.table() : state.tableName;
        List<Record> records = storage.scanTable(table);

        List<Integer> rids = state.tree.search(key);
        List<Record> results = new ArrayList<>();
        for (int rid : rids) {
            results.add(records.get(rid));
        }
        return results;
    }

    // To be called by StorageManager after a successful insert
    public void onTableInsert(String tableName, Record newRecord) {
        int rid = tableRowCounts.getOrDefault(tableName, 0);
        for (IndexState state : indexStates.values()) {
            if (!state.tableName.equals(tableName)) continue;
            Object val = newRecord.getValues().get(state.columnIndex);
            if (!(val instanceof Integer)) {
                throw new IllegalStateException("Indexed column expected INT but found: " + (val == null ? "null" : val.getClass().getSimpleName()));
            }
            state.tree.insert((Integer) val, rid);
        }
        tableRowCounts.put(tableName, rid + 1);
    }

    // Helper: find column index by name, returns -1 if not found.
    private int findColumnIndex(List<ColumnSchema> cols, String name) {
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).name().equals(name)) return i;
        }
        return -1;
    }

    // Runtime index state holder
    private static final class IndexState {
        final String tableName;
        final int columnIndex;
        final BPlusTree tree;

        IndexState(String indexName, String tableName, String columnName, int columnIndex, BPlusTree tree) {
            this.tableName = tableName;
            this.columnIndex = columnIndex;
            this.tree = tree;
        }
    }
}
