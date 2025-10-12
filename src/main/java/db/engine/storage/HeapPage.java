package db.engine.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import db.engine.catalog.ColumnSchema;

/**
 * Mutable heap page with simple header + slot directory layout.
 *
 * Layout (pageSize bytes):
 * [0..3]   int  freeSpacePointer  (start of next record bytes)
 * [4..5]   short slotCount        (number of active slots)
 * [6..7]   short reserved
 * [8..... freeSpacePointer-1]    record data area (variable-length records packed front-to-back)
 * [... free space ...]
 * [slot directory entries growing backward from end of page]
 *
 * Each slot entry = 4 bytes:
 *   short recordOffset  (start of record bytes within page)
 *   short recordLength  (length of record bytes)
 *
 * Free space = (startOfSlotDir) - freeSpacePointer.
 * We DO NOT compact or reuse tombstoned slots yet.
 */
public final class HeapPage {
    private static final int HEADER_SIZE = 8;
    private static final int SLOT_ENTRY_SIZE = 4;
    private static final short TOMBSTONE = -1;

    private final String filePath;
    private final int pageId;
    private final byte[] data;         // backing store (page-sized)
    private final int pageSize;

    private HeapPage(String filePath, int pageId, byte[] data, int pageSize) {
        this.filePath = filePath;
        this.pageId = pageId;
        this.data = data;
        this.pageSize = pageSize;
    }

    public static HeapPage wrap(String filePath, int pageId, byte[] data, int pageSize) {
        // If freshly allocated (all zeros) initialize header explicitly
        if (readFreePtr(data) == 0 && readSlotCount(data) == 0) {
            writeFreePtr(data, HEADER_SIZE); // first record placed after header
            writeSlotCount(data, (short) 0);
        }
        return new HeapPage(filePath, pageId, data, pageSize);
    }

    public boolean canFit(int recordLen) {
        if (recordLen > 0xFFFF) return false; // must fit in unsigned short
        int freePtr = readFreePtr(data);
        int slotCount = readSlotCount(data);
        int startOfNextSlotDir = pageSize - (slotCount * SLOT_ENTRY_SIZE);
        int freeBytes = startOfNextSlotDir - freePtr;
        int needed = recordLen + SLOT_ENTRY_SIZE;
        return needed <= freeBytes;
    }

    /** Insert record bytes; returns slotId. */
    public int insert(byte[] recordBytes) {
        int len = recordBytes.length;
        if (!canFit(len)) throw new IllegalStateException("Not enough space to insert record len=" + len);
        int freePtr = readFreePtr(data);
        int slotCount = readSlotCount(data);

        // Copy record to page
        System.arraycopy(recordBytes, 0, data, freePtr, len);

        // Write slot entry
        int newSlotId = slotCount; // next slot index
        int slotWritePos = slotEntryPos(newSlotId);
        // Write record offset and length into slot
        putShort(data, slotWritePos, (short) freePtr);
        putShort(data, slotWritePos + 2, (short) len);

        // Update header
        writeFreePtr(data, freePtr + len);
        writeSlotCount(data, (short) (slotCount + 1));

        return newSlotId;
    }

    /** Read record bytes from a slot (ignores tombstoned slots). */
    public byte[] readSlot(int slotId) {
        int slotCount = readSlotCount(data);
        if (slotId < 0 || slotId >= slotCount) {
            throw new IllegalArgumentException("slotId out of range: " + slotId);
        }
        int slotPos = slotEntryPos(slotId);
        short offset = getShort(data, slotPos);
        if (offset == TOMBSTONE) {
            throw new IllegalStateException("Slot " + slotId + " is tombstoned");
        }
        short len = getShort(data, slotPos + 2);
        if (len <= 0) {
            throw new IllegalStateException("Slot " + slotId + " is empty");
        }
        byte[] out = new byte[len];
        // Read record bytes from page into out
        System.arraycopy(data, offset, out, 0, len);
        return out;
    }

    /** Deserialize a record given slotId and column schema */
    public db.engine.storage.Record readRecord(int slotId, List<ColumnSchema> columns) {
        return db.engine.storage.Record.fromBytes(readSlot(slotId), columns);
    }

    /** Tombstone a slot. Space not reclaimed until compaction (future impementation). */
    public void delete(int slotId) {
        int slotCount = readSlotCount(data);
        if (slotId < 0 || slotId >= slotCount) return;
        int slotPos = slotEntryPos(slotId);
        putShort(data, slotPos, TOMBSTONE);
        putShort(data, slotPos + 2, (short) 0);
    }

    /** Returns list of live (non-tombstoned) slot ids in ascending order. */
    public List<Integer> liveSlotIds() {
        int slotCount = readSlotCount(data);
        List<Integer> out = new ArrayList<>();
        for (int i = 0; i < slotCount; i++) {
            int slotPos = slotEntryPos(i);
            short off = getShort(data, slotPos);
            short len = getShort(data, slotPos + 2);
            if (off != TOMBSTONE && len > 0) out.add(i);
        }
        return out;
    }

    /**
     * Compute the byte offset of the slot directory entry for a given slotId.
     * Slot directory grows backwards from the end of the page; slot 0 is stored
     * in the last 4 bytes, slot 1 just before that, etc.
     */
    private int slotEntryPos(int slotId) {
        return pageSize - ((slotId + 1) * SLOT_ENTRY_SIZE);
    }

    public byte[] rawData() { return data; }
    public int pageId() { return pageId; }
    public String filePath() { return filePath; }

    private static int readFreePtr(byte[] d) { return ByteBuffer.wrap(d, 0, 4).getInt(); }
    private static void writeFreePtr(byte[] d, int v) { ByteBuffer.wrap(d, 0, 4).putInt(v); }
    private static short readSlotCount(byte[] d) { return ByteBuffer.wrap(d, 4, 2).getShort(); }
    private static void writeSlotCount(byte[] d, short v) { ByteBuffer.wrap(d, 4, 2).putShort(v); }

    private static short getShort(byte[] d, int pos) { return ByteBuffer.wrap(d, pos, 2).getShort(); }
    private static void putShort(byte[] d, int pos, short v) { ByteBuffer.wrap(d, pos, 2).putShort(v); }

    @Override
    public String toString() {
        return "HeapPage{id=" + pageId +
               ", freePtr=" + readFreePtr(data) +
               ", slotCount=" + readSlotCount(data) +
               ", file='" + filePath + "'}";
    }
}
