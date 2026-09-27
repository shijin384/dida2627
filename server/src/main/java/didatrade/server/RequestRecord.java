package didatrade.server;

public class RequestRecord {
    public enum Status {
        PENDING,      // received, not yet handed to Paxos
        IN_PROGRESS,  // assigned a Paxos instance, awaiting decision
        PROCESSED     // decided, applied, response set
    }

    private int requestid;
    private DidaTradeCommand request;
    private boolean response_available;
    private boolean response_value;
    private Status status;
    private int assigned_instance;

    public RequestRecord(int id) {
        this.requestid = id;
        this.request = null;
        this.response_available = false;
        this.response_value = false;
        this.status = Status.PENDING;
        this.assigned_instance = -1;
    }

    public RequestRecord(int id, DidaTradeCommand rq) {
        this.requestid = id;
        this.request = rq;
        this.response_available = false;
        this.response_value = false;
        this.status = Status.PENDING;
        this.assigned_instance = -1;
    }

    public DidaTradeCommand getRequest() {
        return this.request;
    }

    public int getId() {
        return this.requestid;
    }

    public void setRequest(DidaTradeCommand rq) {
        this.request = rq;
    }

    public boolean getResponseValue() {
        return this.response_value;
    }

    public synchronized void setResponse(boolean resp) {
        this.response_value = resp;
        this.response_available = true;
        this.notifyAll();
    }

    public synchronized boolean waitForResponse() {
        while (this.response_available == false) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
        }
        return this.response_value;
    }


    public synchronized Status getStatus() {
        return this.status;
    }

    public synchronized void markInProgress(int instance) {
        this.status = Status.IN_PROGRESS;
        this.assigned_instance = instance;
    }

    public synchronized void markPendingAgain() {
        this.status = Status.PENDING;
        this.assigned_instance = -1;
    }

    public synchronized int getAssignedInstance() {
        return this.assigned_instance;
    }
}