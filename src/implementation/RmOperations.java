package implementation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import schema.Replica;
import schema.ReplicaManager;

public class RmOperations {
	// keep logging everything
    private Logger logs;
    // keeps track of replicas against their campus codes as key
    private HashMap<String, Replica> replicaList = new HashMap<>();
    // keeps track of all the replica managers in network
    private List<ReplicaManager> replicaManagers = new ArrayList<>();
    // keep track of the system failure
    private int failure = 0;
    // keep track of the last failure state (the sequence number)
    private int lastFailedSequence = 1;

    public RmOperations(String replicaManagers, String replicas, Logger logs) {
        this.logs = logs;

        // parse the replica list (format: name,code,port;)
        String[] replicaList = replicas.split(";");
        for (String item : replicaList) {
            // parse all the parameters
            String[] params = item.split(",");
            Replica replica = new Replica(Integer.parseInt(params[2]), params[0], params[3]);
            // add to the list
            this.replicaList.put(params[1], replica);
        }

        // parse the replica manager list (format: ipAddress,port;)
        String[] replicaManagerList = replicaManagers.split(";");
        for (String item : replicaManagerList) {
            // parse all the parameters
            String[] params = item.split(",");
            ReplicaManager manager = new ReplicaManager(params[0], Integer.parseInt(params[1]));
            // add to the list
            this.replicaManagers.add(manager);
        }
    }

    // get the replica port number based on the campus code
    int getReplicaPort(String code) {
        Replica replica = replicaList.getOrDefault(code, null);
        return (replica == null) ? -1 : replica.getUdpPort();
    }

    // start the replica servers
    public void startReplicas() {
        this.logs.info("Starting all the replica servers");
        for (Map.Entry<String, Replica> replicaEntry : this.replicaList.entrySet()) {
            String code = replicaEntry.getKey();
            this.startReplica(code);      
        }
    }

    // increment the replica failure count
    void incrementFailureCount(String code) {
        Replica replica = this.replicaList.getOrDefault(code, null);
        if (replica != null)
            replica.incrementFailureCount();
    }
    
    /**
     * increment the system failure count
     * @param sequence defines the sequence number of the point of failure 
     */
    void incrementFailureCount(int sequence) {
    	this.failure = (sequence == (this.lastFailedSequence + 1)) ? this.failure + 1 : 0;
    	this.lastFailedSequence = sequence;
    }
    
    // increment the system failure count
    void incrementFailureCount() {
    	this.failure += 1;
    }

    // check if the replica fails for three consecutive time
    boolean isReplicaFailureCritical(String code) {
        Replica replica = this.replicaList.getOrDefault(code, null);
        return (replica != null) && replica.isFailureCountCritical();
    }
    
    // check if the system fails for three consecutive time
    boolean isSystemCritical() {
    	return (this.failure >= 3);
    }
    
    // reset the failure count
    void resetFailureCount() {
    	this.failure = 0;
    }

    // decrement the replica failure count (but it should not go below zero)
    void decrementFailureCount(String code) {
        Replica replica = this.replicaList.getOrDefault(code, null);
        if (replica != null)
            replica.decrementFailureCount();
    }

    // start a replica
    void startReplica(String code) {
        // find the replica
        Replica replica = this.replicaList.getOrDefault(code, null);

        // no replica. no process to execute
        if (replica == null) return;

        // list of commands to execute
        String command = "java -cp \"G:\\workspace\\drrs-ftha-replica\\bin\" " + replica.path;
        
        System.out.println(command);

        ReplicaThread rThread = new ReplicaThread(command, replica, code, this.replicaList, this.logs);
        rThread.start();
    }
    
    void killReplicas() {
    	for (Map.Entry<String, Replica> replicaEntry : this.replicaList.entrySet()) {
    		Replica replica = replicaEntry.getValue();
    		
    		if (replica.getProcess().isAlive())
    			replica.reset();
    	}
    }

    // kill a replica server
    void killReplica(String code) {
        Replica replica = this.replicaList.get(code);
        // because, what is dead can not die again.
        if (replica.getProcess().isAlive())
            replica.reset();
    }

    // fetch the details of all the replica managers
    List<ReplicaManager> getReplicaManagers() {
        return replicaManagers;
    }

    // operation code for listening to request from replica for importing data
    static final int R_REQ_IMPORT = 7;

    // operation code for sending the request to replica to export their data
    static final int R_REQ_EXPORT = 8;

    // operation code for sending and listening the request to other RMs to import data from their replica
    static final int RM_REQ_IMPORT = 0;

    // key string for sending campus code in request body
    static final String BODY_CODE = "c";

    // operation code for failure of operation by replica from FE
    static final int FE_FAIL = 3;

    // operation code for success of operation by replica from FE
    static final int FE_SUCCESS = 4;

    // key string for sending room records in request body
    static final String BODY_ROOM_RECORD = "rr";
}
