package didatrade.server;

import java.util.LinkedHashMap;
import java.util.Map;

public class RequestHistory {
    private final Map<Integer, RequestRecord> pending;
    private final Map<Integer, RequestRecord> processed;

    public RequestHistory() {
        this.pending = new LinkedHashMap<Integer, RequestRecord>();
        this.processed = new LinkedHashMap<Integer, RequestRecord>();
    }

    public synchronized RequestRecord getIfPending(int requestid) {
        return this.pending.get(requestid);
    }

    // The actual fix: skip anything already IN_PROGRESS (already assigned
    // an instance, awaiting decision) instead of blindly returning
    // whatever the map iterates to first.
    public synchronized RequestRecord getFirstPending() {
        for (RequestRecord record : this.pending.values()) {
            if (record.getStatus() == RequestRecord.Status.PENDING)
                return record;
        }
        return null;
    }

    public synchronized RequestRecord getIfProcessed(int requestid) {
        return this.processed.get(requestid);
    }

    public synchronized RequestRecord getIfExists(int requestid) {
        RequestRecord record = this.pending.get(requestid);
        if (record == null)
            record = this.processed.get(requestid);
        return record;
    }

    public synchronized void addToPending(int requestid, RequestRecord record) {
        this.pending.put(requestid, record);
    }

    public synchronized RequestRecord moveToProcessed(int requestid) {
        RequestRecord record = this.pending.remove(requestid);
        this.processed.put(requestid, record);
        return record;
    }
}