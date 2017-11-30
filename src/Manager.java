import implementation.RmOperations;
import implementation.UdpThread;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

public class Manager {
    public static void main(String[] args) {
        RmOperations rmOps;
        String replicaList, replicaManagerList;
        Logger logs = Logger.getLogger("replica-manager");

        // check for the arguments
        if (args.length != 2) {
            System.out.println("Usage: java Manager <replica-directory> <manager-directory>\nWhere, <replica-directory> is the list of the replicas. and <replica-directory> is the list of replica managers");
            return;
        }

        // initialize the logger file
        try {
            FileHandler fileHandler = new FileHandler("replica-manager.log", true);
            logs.addHandler(fileHandler);
        } catch(IOException ioe) {
            logs.warning("Failed to create handler for log file.\n Message: " + ioe.getMessage());
        }

        // get the node details from the file
        try {
            BufferedReader replicaReader = new BufferedReader(new FileReader(args[0]));
            BufferedReader rmReader = new BufferedReader(new FileReader(args[1]));
            replicaList = replicaReader.readLine();
            replicaManagerList = rmReader.readLine();
        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
            return;
        }

        // initialize replica manager implementation
        rmOps = new RmOperations(replicaManagerList, replicaList, logs);

        // start the udp server
        try {
            DatagramSocket udpSocket = new DatagramSocket(8034);
            byte[] incoming = new byte[10000];
            logs.info("The UDP server for replica manager is up and running on port 8034");

            // start all the replicas
            rmOps.startReplicas();

            while (true) {
                DatagramPacket packet = new DatagramPacket(incoming, incoming.length);
                try {
                    udpSocket.receive(packet);
                    UdpThread thread = new UdpThread(logs, udpSocket, packet, rmOps);
                    thread.start();
                } catch (IOException ioe) {
                    logs.warning("Exception thrown while receiving packet.\nMessage: " + ioe.getMessage());
                }
                if (udpSocket.isClosed())
                    break;
            }
        } catch (SocketException e) {
            logs.warning("Exception thrown while server was running/trying to start.\nMessage: " + e.getMessage());
        }
    }
}