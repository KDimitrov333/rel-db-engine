package db.engine.index;

import db.engine.catalog.CatalogManager;
import db.engine.catalog.IndexSchema;
import db.engine.catalog.TableSchema;
import db.engine.catalog.ColumnSchema;
import db.engine.catalog.DataType;
import db.engine.storage.StorageManager;
import db.engine.storage.RID;
import db.engine.storage.Record;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class IndexManager {
    private CatalogManager catalog;
    private StorageManager storage;
    private final Map<String, IndexState> indexStates;

    public IndexManager(CatalogManager catalog, StorageManager storage) {
        this.catalog = catalog;
        this.storage = storage;
        this.indexStates = new HashMap<>();
        this.storage.attachIndexManager(this);
    }

    // Create index for a table column (only INT supported for now)
    public void createIndex(String indexName, String tableName, String columnName) {
        TableSchema tSchema = catalog.getTableSchema(tableName);
        if (tSchema == null) throw new IllegalArgumentException("Table not found: " + tableName);

        List<ColumnSchema> cols = tSchema.columns();
        int colIndex = findColumnIndex(cols, columnName);
        if (colIndex == -1) throw new IllegalArgumentException("Column not found: " + columnName);
        if (cols.get(colIndex).type() != DataType.INT) {
            throw new IllegalArgumentException("Indexing only supported on INT columns");
        }

    // Build in-memory B+ tree using RIDs via storage heap scan
        BPlusTree tree = new BPlusTree(4);
        storage.scan(tableName, (pageRid, rec) -> {
            Object v = rec.getValues().get(colIndex);
            if (!(v instanceof Integer)) {
                throw new IllegalStateException("Indexed column expected INT but found: " + (v == null ? "null" : v.getClass().getSimpleName()));
            }
            tree.insert((Integer) v, pageRid);
        });

        indexStates.put(indexName, new IndexState(indexName, tableName, columnName, colIndex, tree));

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
        var rids = state.tree.search(key);
        List<Record> out = new ArrayList<>(rids.size());
    for (RID rid : rids) {
            out.add(storage.read(table, rid));
        }
        return out;
    }

    /**
     * Range lookup using an index. Returns all records whose indexed key is in [lowInclusive, highInclusive].
     * If the range is empty (low > high) an empty list is returned.
     */
    public List<Record> rangeLookup(String indexName, int lowInclusive, int highInclusive) {
        if (lowInclusive > highInclusive) return List.of();
        IndexState state = indexStates.get(indexName);
        if (state == null) throw new IllegalArgumentException("Index not found: " + indexName);

        IndexSchema iSchema = catalog.getIndexSchema(indexName);
        String table = (iSchema != null) ? iSchema.table() : state.tableName;
        var rids = state.tree.rangeSearch(lowInclusive, highInclusive);
        List<Record> out = new ArrayList<>(rids.size());
    for (RID rid : rids) {
            out.add(storage.read(table, rid));
        }
        return out;
    }

    // To be called by StorageManager after a successful insert
    public void onTableInsert(String tableName, RID rid, Record newRecord) {
        for (IndexState state : indexStates.values()) {
            if (!state.tableName.equals(tableName)) continue;
            Object val = newRecord.getValues().get(state.columnIndex);
            if (!(val instanceof Integer)) {
                throw new IllegalStateException("Indexed column expected INT but found: " + (val == null ? "null" : val.getClass().getSimpleName()));
            }
            state.tree.insert((Integer) val, rid);
        }
    }

    // To be called by StorageManager after a deletion; oldRecord supplies the key for removal.
    public void onTableDelete(String tableName, RID rid, Record oldRecord) {
        if (oldRecord == null) return; // safety
        for (IndexState state : indexStates.values()) {
            if (!state.tableName.equals(tableName)) continue;
            Object val = oldRecord.getValues().get(state.columnIndex);
            if (val instanceof Integer iv) {
                state.tree.delete(iv, rid);
            }
        }
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
