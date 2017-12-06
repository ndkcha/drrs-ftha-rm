package implementation;

import schema.Replica;
import schema.ReplicaManager;
import schema.UdpPacket;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
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

    // send data to replica
    public void sendDataToReplica(String code, int replicaPort) {
        try {
            // for incoming packets
            byte[] inBuffer = new byte[10000];
            DatagramPacket incoming = new DatagramPacket(inBuffer, inBuffer.length);

            // new socket to keep track of everything
            DatagramSocket socket = new DatagramSocket();

            // make the packet
            HashMap<String, Object> body = new HashMap<>();
            body.put(BODY_CODE, code);
            UdpPacket udpPacket = new UdpPacket(RM_REQ_IMPORT, body);
            byte[] outgoing = this.serialize(udpPacket);

            // send the other RM for the data
            DatagramPacket outPacket = new DatagramPacket(outgoing, outgoing.length, InetAddress.getByName(""), 8034);
            socket.send(outPacket);

            // get the data from the RM
            socket.receive(incoming);

            byte[] toReplica = incoming.getData();
            DatagramPacket toReplicaPacket = new DatagramPacket(toReplica, toReplica.length, InetAddress.getByName("localhost"), replicaPort);
            socket.send(toReplicaPacket);
        } catch (SocketException exception) {
            this.logs.warning("Error connecting to other RMs\nMessage: " + exception.getMessage());
        } catch (IOException exception) {
            this.logs.warning("Error encoding the packet.\nMessage: " + exception.getMessage());
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

    private byte[] serialize(Object obj) throws IOException {
        try(ByteArrayOutputStream b = new ByteArrayOutputStream()){
            try(ObjectOutputStream o = new ObjectOutputStream(b)){
                o.writeObject(obj);
            }
            return b.toByteArray();
        }
    }

    private Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try(ByteArrayInputStream b = new ByteArrayInputStream(bytes)){
            try(ObjectInputStream o = new ObjectInputStream(b)){
                return o.readObject();
            }
        }
    }
}
