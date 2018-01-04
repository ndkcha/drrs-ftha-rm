package schema;

import java.io.Serializable;
import java.util.HashMap;

public class UdpPacket implements Serializable {
	private static final long serialVersionUID = 1L;
    public int operation, fePort, sequence;
    public HashMap<String, Object> body;

    public UdpPacket(int operation, HashMap<String, Object> body) {
        this.operation = operation;
        this.body = body;
    }
}
