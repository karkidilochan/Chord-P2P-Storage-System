package csx55.chords.utils;

public class Entry {
    /* this is the ring position */
    long start;

    /* ipAddress:port string */
    String ipAddress;

    int port;

    public Entry(long start, String ipAddress, int port) {
        this.start = start;
        this.ipAddress = ipAddress;
        this.port = port;
    }

    public String getEntryString() {
        return ipAddress + ":" + port;
    }

    public int getHashCode() {
        return Math.abs(getEntryString().hashCode());
    }

    public String getAddress() {
        return ipAddress;
    }

    public int getPort() {
        return port;
    }

    public void updateEntry(String ipAddress, int port) {
        this.ipAddress = ipAddress;
        this.port = port;
    }

    public long getRingPosition() {
        return start;
    }

}
