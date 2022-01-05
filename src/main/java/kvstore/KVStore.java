package kvstore;

import wal.WALConfig;
import wal.WALEntry;
import wal.WriteAheadLog;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KVStore {

    private final ScheduledExecutorService singleThreadedExecutor = Executors.newSingleThreadScheduledExecutor();

    private Map<String, String> kv = new HashMap<>();
    private WriteAheadLog wal;

    public KVStore(WALConfig config) {
        startup(config);
        this.wal = WriteAheadLog.openWAL(config);
        this.applyLog();
    }

    public void startup(WALConfig config) {
        scheduleLogCleaning(config);
    }

    private void scheduleLogCleaning(WALConfig config) {
        singleThreadedExecutor.schedule(() -> {
            cleanLogs();
        }, config.getCleanTaskIntervalMs(), TimeUnit.MILLISECONDS);
    }

    public String get(String key) {
        return kv.get(key);
    }

    public void put(String key, String value) {
        appendLog(key, value);
        kv.put(key, value);
    }

    public void remove(String key) {
        appendLog(key);
        kv.remove(key);
    }

    private void appendLog(String key) {
        wal.writeEntry(new RemoveValueCommand(key).serialize());
    }

    private Long appendLog(String key, String value) {
        return wal.writeEntry(new SetValueCommand(key, value).serialize());
    }

    private void applyLog() {
        List<WALEntry> walEntries = wal.readAll();
        applyEntries(walEntries);
    }

    private void applyEntries(List<WALEntry> walEntries) {
        walEntries.forEach(entry -> {
            Command command = deserialize(new ByteArrayInputStream(entry.getData()));
            if (command instanceof SetValueCommand) {
                SetValueCommand setValueCommand = (SetValueCommand) command;
                kv.put(setValueCommand.getKey(), setValueCommand.getValue());
            }
        });
    }

    private static Command deserialize(InputStream is) {
        try {
            DataInputStream dataInputStream = new DataInputStream(is);
            int type = dataInputStream.readInt();
            if (type == Command.SetValueType) {
                return new SetValueCommand(dataInputStream.readUTF(), dataInputStream.readUTF());
            } else if (type == Command.RemoveValueType) {
                return new RemoveValueCommand(dataInputStream.readUTF());
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initialiseFromSnapshot(Snapshot snapShot) {
        kv.putAll(snapShot.deserializeState());
    }

    private Snapshot takeSnapshot() {
        Long snapShotTakenAtLogIndex = wal.getLastLogEntryId();
        return new Snapshot(serializeState(kv), snapShotTakenAtLogIndex);
    }

    private long timeElaspedSince(long now, long lastLogEntryTimestamp) {
        return now - lastLogEntryTimestamp;
    }

    public File serializeState(Map<String, String> kv) {
        return null;
    }

    private void cleanLogs() {
        // take snapshot and clean up logs
    }
}

