package db.engine.exec;

import java.util.Iterator;
import java.util.List;
import java.io.File;
import java.io.IOException;

import db.engine.catalog.TableSchema;
import db.engine.catalog.ColumnSchema;
import db.engine.storage.Record;
import db.engine.storage.RID;
import db.engine.storage.StorageManager;
import db.engine.storage.HeapPage;

/**
 * Physical operator that performs a full table scan
 * Used when no index is applied or when the query requests all rows
 */
public class SeqScanOperator implements Operator {
    private final StorageManager storage;
    private final String tableName;

    // Metadata & state
    private TableSchema tableSchema;
    private List<ColumnSchema> columns;
    private int pageCount;
    private int currentPageId;
    private Iterator<Integer> currentSlotIter;
    private HeapPage currentHeapPage;
    private boolean opened;

    public SeqScanOperator(StorageManager storage, String tableName) {
        this.storage = storage;
        this.tableName = tableName;
    }

    @Override
    public void open() {
        tableSchema = storage.getCatalog().getTableSchema(tableName);
        if (tableSchema == null) throw new IllegalArgumentException("Unknown table: " + tableName);
        columns = tableSchema.columns();
        File f = new File(tableSchema.filePath());
        long len = f.exists() ? f.length() : 0L;
        pageCount = len == 0 ? 0 : (int) ((len + StorageManager.PAGE_SIZE - 1) / StorageManager.PAGE_SIZE);
        currentPageId = 0;
        currentSlotIter = null;
        currentHeapPage = null;
        opened = true;
    }

    @Override
    public Row next() {
        if (!opened) return null;
        while (true) {
            if (currentSlotIter == null || !currentSlotIter.hasNext()) {
                if (currentPageId >= pageCount) return null; // done
                loadPage(currentPageId++);
            }
            if (currentSlotIter != null && currentSlotIter.hasNext()) {
                int slotId = currentSlotIter.next();
                Record rec = currentHeapPage.readRecord(slotId, columns);
                return Row.of(rec, new RID(currentHeapPage.pageId(), slotId), columns);
            }
        }
    }

    private void loadPage(int pageId) {
        byte[] bytes;
        try {
            bytes = storage.getBufferManager().getPage(tableSchema.filePath(), pageId).data();
        } catch (IOException e) {
            throw new RuntimeException("Failed loading page " + pageId + " for table " + tableName, e);
        }
        currentHeapPage = HeapPage.wrap(tableSchema.filePath(), pageId, bytes, StorageManager.PAGE_SIZE);
        currentSlotIter = currentHeapPage.liveSlotIds().iterator();
    }

    @Override
    public void close() {
        opened = false;
        currentHeapPage = null;
        currentSlotIter = null;
        columns = null;
        tableSchema = null;
    }
    
    @Override
    public List<ColumnSchema> schema() { return columns; }
}
