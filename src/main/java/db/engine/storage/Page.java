package db.engine.storage;

/**
 * Immutable view of a fixed-size page loaded from a heap file.
 */
public final class Page {
    private final String filePath;
    private final int pageId;
    private final byte[] data; // page-size buffer

    public Page(String filePath, int pageId, byte[] data) {
        this.filePath = filePath;
        this.pageId = pageId;
        this.data = data;
    }

    public String filePath() { return filePath; }
    public int pageId() { return pageId; }
    public byte[] data() { return data; }
}
