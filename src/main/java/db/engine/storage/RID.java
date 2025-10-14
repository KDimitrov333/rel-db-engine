package db.engine.storage;

/**
 * Row Identifier: identifies a record by (pageId, slotId) within a table's heap file.
 */
public record RID(int pageId, int slotId) {
	@Override
	public String toString() {
		// For debugging
		return "(" + pageId + "," + slotId + ")";
	}
}
