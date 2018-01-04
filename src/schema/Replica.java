package schema;

public class Replica {
	private int udpPort, failures;
    public String name, path;
    private Process process;

    public Replica(int udpPort, String name, String path) {
        this.udpPort = udpPort;
        this.name = name;
        this.path = path;
        this.failures = 0;
    }

    public void reset() {
        this.process.destroy();
        this.process = null;
        this.failures = 0;
    }

    public void setProcess(Process process) {
        this.process = process;
    }

    public void incrementFailureCount() {
        this.failures += 1;
    }

    public void decrementFailureCount() {
        this.failures = 0;
    }

    public boolean isFailureCountCritical() {
        return (this.failures >= 3);
    }

    public int getUdpPort() {
        return udpPort;
    }

    public Process getProcess() {
        return process;
    }
}
