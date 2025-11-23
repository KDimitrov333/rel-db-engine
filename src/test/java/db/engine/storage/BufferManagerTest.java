package db.engine.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.RandomAccessFile;
import org.junit.jupiter.api.Test;

public class BufferManagerTest {

    @Test
    void loadsExistingPageAndCachesIt() throws Exception {
        File temp = File.createTempFile("buf-test", ".tbl");
        temp.deleteOnExit();
        try (RandomAccessFile raf = new RandomAccessFile(temp, "rw")) {
            byte[] payload = new byte[4096];
            payload[0] = 42; // marker
            raf.write(payload);
        }
        BufferManager bm = new BufferManager(4096, 8);
        Page p1 = bm.getPage(temp.getPath(), 0);
        assertEquals(42, p1.data()[0]);
        Page p2 = bm.getPage(temp.getPath(), 0); // cached
        assertSame(p1, p2);
    }

    @Test
    void beyondEOFReturnsZeroedPage() throws Exception {
        File temp = File.createTempFile("buf-test-empty", ".tbl");
        temp.deleteOnExit();
        BufferManager bm = new BufferManager(4096, 4);
        Page p = bm.getPage(temp.getPath(), 3); // pageId 3 beyond EOF
        for (byte b : p.data()) assertEquals(0, b);
    }
}
