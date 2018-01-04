import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import implementation.RmOperations;
import implementation.UdpThread;

public class Manager {
	public static void main(String[] args) {
		RmOperations rmOps;
		String replicaList = "Dorval,DVL,8022,CampusServer 0;Kirkland,KKL,8032,CampusServer 1;Westmount,WST,8042,CampusServer 2";
		String replicaManagerList = "132.205.93.42,8020;192.168.1.24,8020";
		Logger logs = Logger.getLogger("replica-manager");
		
		// initialize the logger file
		try {
			FileHandler fileHandler = new FileHandler("replica-manager.log", true);
			logs.addHandler(fileHandler);
		} catch (IOException ioe) {
			logs.warning("Failed to create handler for log file.\nMessage: " + ioe.getMessage());
		}
		
		// initialize replica manager implementation
		rmOps = new RmOperations(replicaManagerList, replicaList, logs);
		
		// start the udp server
		try {
			DatagramSocket udpSocket = new DatagramSocket(8020);
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
			logs.warning("Exception thrown while server was runnning/trying to start.\nMessage: " + e.getMessage());
		}
	}
}
