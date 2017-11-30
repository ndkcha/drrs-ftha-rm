package implementation;

import schema.ReplicaManager;
import schema.TimeSlot;
import schema.UdpPacket;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Logger;

public class UdpThread implements Runnable {
    private Thread thread;
    private Logger logs;
    private DatagramSocket socket;
    private DatagramPacket packet;
    private RmOperations rmOps;

    public UdpThread(Logger logs, DatagramSocket socket, DatagramPacket packet, RmOperations rmOps) {
        this.logs = logs;
        this.socket = socket;
        this.packet = packet;
        this.rmOps = rmOps;
    }

    @Override
    public void run() {
        try {
            UdpPacket udpPacket = (UdpPacket) deserialize(this.packet.getData());
            byte[] outwards;

            switch (udpPacket.operation) {
                case RmOperations.R_REQ_IMPORT:
                    outwards = this.replicaRequestsData(udpPacket.body);
                    break;
                case RmOperations.RM_REQ_IMPORT:
                    outwards = this.rmRequestsData(udpPacket.body);
                    break;
                case RmOperations.FE_FAIL:
                    this.replicaFails(udpPacket.body);
                    return;
                case RmOperations.FE_SUCCESS:
                    this.replicaSucceeds(udpPacket.body);
                    return;
                default:
                    outwards = this.serialize("Error");
                    break;
            }

            DatagramPacket response = new DatagramPacket(outwards, outwards.length, this.packet.getAddress(), this.packet.getPort());
            this.socket.send(response);
        } catch (IOException ioe) {
            logs.warning("Error reading the packet.\nMessage: " + ioe.getMessage());
        } catch (ClassNotFoundException e) {
            logs.warning("Error parsing the packet.\nMessage: " + e.getMessage());
        }
    }

    // when the replica gives different response to front end. i.e. replica execution is unique at front end.
    private void replicaFails(HashMap<String, Object> body) {
        String code = (String) body.get(RmOperations.BODY_CODE);

        this.rmOps.incrementFailureCount(code);

        if (this.rmOps.isReplicaFailureCritical(code)) {
            this.rmOps.killReplica(code);
            this.rmOps.startReplica(code);
        }
    }

    // when the replica gives the same response to the front end as other hosts' replica. i.e. replica execution is not unique at front end.
    private void replicaSucceeds(HashMap<String, Object> body) {
        String code = (String) body.get(RmOperations.BODY_CODE);
        this.rmOps.decrementFailureCount(code);
    }

    // when replica sends request to fetch data from other nodes in the network
    // in that case, send a request to other RMs about the data and wait for their responses.
    @SuppressWarnings(value = "unchecked")
    private byte[] replicaRequestsData(HashMap<String, Object> body) throws IOException {
        HashMap<String, HashMap<Integer, List<TimeSlot>>> mapToSend = new HashMap<>();

        try {
            // keeps track of incoming data
            List<HashMap<String, HashMap<Integer, List<TimeSlot>>>> data = new ArrayList<>();

            // for incoming packets
            byte[] inBuffer = new byte[10000];
            DatagramPacket incoming = new DatagramPacket(inBuffer, inBuffer.length);

            // new socket to keep track of everything
            DatagramSocket socket = new DatagramSocket();

            // make the packet
            UdpPacket packet = new UdpPacket(RmOperations.RM_REQ_IMPORT, body);

            // make packet and send to all other RMs
            byte[] outgoing = this.serialize(packet);
            for (ReplicaManager manager : this.rmOps.getReplicaManagers()) {
                DatagramPacket datagramPacket = new DatagramPacket(outgoing, outgoing.length, manager.getIpAddress(), manager.getUdpPort());
                socket.send(datagramPacket);
            }

            socket.setSoTimeout(3000);

            while (true) {
                try {
                    socket.receive(incoming);

                    HashMap<String, HashMap<Integer, List<TimeSlot>>> inData = (HashMap<String, HashMap<Integer, List<TimeSlot>>>) this.deserialize(incoming.getData());
                    data.add(inData);

                    if (data.size() == 3)
                        break;
                } catch (SocketTimeoutException exception) {
                    this.logs.info("Connections to Replica Manager timed out.");
                    break;
                } catch (ClassNotFoundException exception) {
                    this.logs.warning("Could not parse incoming data from Replica Manager.\nMessage: " + exception.getMessage());
                }
            }

            // compare all the structure. (to be discussed and implemented)

            // send the first one in the map
            mapToSend = data.iterator().next();
        } catch (SocketException exception) {
            this.logs.warning("Error connecting to other RMs\nMessage: " + exception.getMessage());
        } catch (IOException exception) {
            this.logs.warning("Error encoding/parsing the packet.\nMessage: " + exception.getMessage());
        }
        return this.serialize(mapToSend);
    }

    // when another replica manager needs data
    // in that case, send the data request to relevant replica and wait for its response
    @SuppressWarnings(value = "unchecked")
    private byte[] rmRequestsData(HashMap<String, Object> body) throws IOException {
        HashMap<String, HashMap<Integer, List<TimeSlot>>> mapToSend = new HashMap<>();
        String code = (String) body.get(RmOperations.BODY_CODE);
        // get the relevant replica port number
        int port = this.rmOps.getReplicaPort(code);
        try {
            // new socket to keep track of the request sequence
            DatagramSocket socket = new DatagramSocket();

            // for incoming data from the socket
            byte[] incoming = new byte[10000];
            DatagramPacket inData = new DatagramPacket(incoming, incoming.length);

            // make the request packet
            UdpPacket packet = new UdpPacket(RmOperations.R_REQ_EXPORT, body);

            // send it to the relevant replica
            byte[] outgoing = this.serialize(packet);
            DatagramPacket outPacket = new DatagramPacket(outgoing, outgoing.length, InetAddress.getByName("localhost"), port);
            socket.send(outPacket);

            // wait for the response
            socket.receive(inData);

            mapToSend = (HashMap<String, HashMap<Integer, List<TimeSlot>>>) this.deserialize(inData.getData());
        } catch (SocketException exception) {
            this.logs.warning("Error connecting to the replica.\nMessage: " + exception.getMessage());
        } catch (IOException exception) {
            this.logs.warning("Error encoding/parsing the packet.\nMessage: " + exception.getMessage());
        } catch (ClassNotFoundException exception) {
            this.logs.warning("Error parsing the packet.\nMessage: " + exception.getMessage());
        }

        return this.serialize(mapToSend);
    }

    public void start() {
        logs.info("One in coming connection. Forking a thread.");
        if (thread == null) {
            thread = new Thread(this, "Udp Process");
            thread.start();
        }
    }

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
