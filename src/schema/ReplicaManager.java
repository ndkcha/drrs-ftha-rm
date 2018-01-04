package schema;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ReplicaManager {
	private String ipAddress;
    private int udpPort;

    public ReplicaManager(String ipAddress, int udpPort) {
        this.ipAddress = ipAddress;
        this.udpPort = udpPort;
    }

    public InetAddress getIpAddress() {
        try {
            return InetAddress.getByName(ipAddress);
        } catch (UnknownHostException unknownHostException) {
            return null;
        }
    }

    public int getUdpPort() {
        return udpPort;
    }
}
