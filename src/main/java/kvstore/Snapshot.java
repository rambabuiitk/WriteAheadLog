package kvstore;

import java.io.File;
import java.util.Map;

public class Snapshot {

    public File file;
    public long lastLogEntry;

    public Snapshot(File snapFile, long lastLogEntry) {
        this.file = snapFile;
        this.lastLogEntry = lastLogEntry;
    }

    public Map<? extends String, ? extends String> deserializeState() {
        return null;
    }

    public File serializeState(Map<String, String> kvstore) {
        return null;
    }

    public File findMostRecentSnapshot() {
        return null;
    }

    public long getLastLogEntry() {
        return lastLogEntry;
    }
}
