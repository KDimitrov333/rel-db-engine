package db.engine.storage;

/**
 * Record Identifier (RID).
 * For the current heap file implementation this is the byte offset (long) in the table file
 * where the record's length prefix begins. In the future, this may be changed to a (pageId, slotId)
 * structure without changing higher-level code that relies on RID as a handle.
 */
public record RID(long offset) {}
