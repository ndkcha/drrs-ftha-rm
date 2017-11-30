package implementation;

import schema.Replica;
import schema.ReplicaManager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class RmOperations {
    // keep logging everything
    private Logger logs;
    // path to java binary
    private final String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
    // keeps track of replicas against their campus codes as key
    private HashMap<String, Replica> replicaList = new HashMap<>();
    // keeps track of all the replica managers in network
    private List<ReplicaManager> replicaManagers = new ArrayList<>();

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
            String[] params = item.split(";");
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

    // check if the replica fails for three consecutive time
    boolean isReplicaFailureCritical(String code) {
        Replica replica = this.replicaList.getOrDefault(code, null);
        return (replica != null) && replica.isFailureCountCritical();
    }

    // decrement the replica failure count (but it should not go below zero)
    void decrementFailureCount(String code) {
        Replica replica = this.replicaList.getOrDefault(code, null);
        if (replica != null)
            replica.decrementFailureCount();
    }

    // start a replica
    void startReplica(String code) {
        // list of commands to execute
        final List<String> command = new ArrayList<>();
        // find the replica
        Replica replica = this.replicaList.getOrDefault(code, null);

        // no replica. no process to execute
        if (replica == null) return;

        // commands used to execute the server
        command.add(javaBin);
        command.add("-jar");
        command.add(replica.path);

        // initialize the process builder
        try {
            final ProcessBuilder builder = new ProcessBuilder(command);
            Process process = builder.start();
            replica.setProcess(process);
            this.logs.info(replica.name + " is up and running. port: " + replica.getUdpPort() + ". code: " + code);
        } catch (IOException ioException) {
            this.logs.warning("The manager could not start the " + replica.name + " server.\nMessage: " + ioException.getMessage());
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

    // operation code for listening to request for importing data from replica
    static final int R_REQ_IMPORT = 0;

    // operation code for sending the request to export their data to replica
    static final int R_REQ_EXPORT = 1;

    // operation code for sending and listening the request to import data from their replica to other RMs
    static final int RM_REQ_IMPORT = 2;

    // key string for sending campus code in request body
    static final String BODY_CODE = "c";

    // operation code for failure of operation by replica from FE
    static final int FE_FAIL = 3;

    // operation code for success of operation by replica from FE
    static final int FE_SUCCESS = 4;
}
