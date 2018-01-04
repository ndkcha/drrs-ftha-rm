package implementation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.logging.Logger;

import schema.Replica;
import schema.UdpPacket;

public class ReplicaThread implements Runnable {
	private Logger logs;
	private Thread thread;
	// list of commands to execute
    String command;
	// the replica to execute
    Replica replica;
    // campus code
    String code;
    // keeps track of replicas against their campus codes as key
    private HashMap<String, Replica> replicaList;
    
    public ReplicaThread(String command, Replica replica, String code, HashMap<String, Replica> replicaList, Logger logs) {
    	this.code = code;
    	this.replica = replica;
    	this.command = command;
    	this.replicaList = replicaList;
    	this.logs = logs;
    }	

	@Override
	public void run() {
        try {
            // start the process
            Process process = Runtime.getRuntime().exec(command);
            // map it for future reference
            replica.setProcess(process);
            replicaList.put(code, replica);
            this.logs.info(replica.name + " is up and running. port: " + replica.getUdpPort() + ". code: " + code);
        } catch (IOException ioException) {
            this.logs.warning("The manager could not start the " + replica.name + " server.\nMessage: " + ioException.getMessage());
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
            body.put(RmOperations.BODY_CODE, code);
            UdpPacket udpPacket = new UdpPacket(RmOperations.RM_REQ_IMPORT, body);
            byte[] outgoing = this.serialize(udpPacket);

            // send the other RM for the data
            DatagramPacket outPacket = new DatagramPacket(outgoing, outgoing.length, InetAddress.getByName(""), 8034);
            socket.send(outPacket);

            // get the data from the RM
            socket.receive(incoming);

            byte[] toReplica = incoming.getData();
            DatagramPacket toReplicaPacket = new DatagramPacket(toReplica, toReplica.length, InetAddress.getByName("localhost"), replicaPort);
            socket.send(toReplicaPacket);
            
            socket.close();
        } catch (SocketException exception) {
            this.logs.warning("Error connecting to other RMs\nMessage: " + exception.getMessage());
        } catch (IOException exception) {
            this.logs.warning("Error encoding the packet.\nMessage: " + exception.getMessage());
        }
    }
	
	public void start() {
        if (thread == null) {
            thread = new Thread(this, "Replica Process");
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

}
