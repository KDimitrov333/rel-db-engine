package db.engine.storage;

/**
 * Simple carrier tying a record to its RID for scans.
 */
public record TableRow(RID rid, Record record) {}
