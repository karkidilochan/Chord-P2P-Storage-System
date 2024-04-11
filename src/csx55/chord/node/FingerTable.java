package csx55.chord.node;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.net.Socket;
import java.util.ArrayList;

import csx55.chord.tcp.TCPConnection;
import csx55.chord.utils.Entry;

public class FingerTable {

    private List<Entry> table = new ArrayList<>();

    private int selfPeerID;

    private String selfAddress;

    private int selfPort;

    private final int FT_ROWS = 32;

    /* for simplicity, keeping predecessor start value -1 */
    private Entry predecessor;

    private Entry successor;

    public Map<String, Integer> fileIndex = new HashMap<>();

    public FingerTable(String selfAddress, int selfPort, int selfPeerID) {
        this.selfPeerID = selfPeerID;
        this.selfAddress = selfAddress;
        this.selfPort = selfPort;
    }

    public List<Entry> getTable() {
        return table;
    }

    // public distance(int firstID, int secondID) {
    // if (firstID <= secondID) {
    // return secondID - firstID;

    // }
    // else {
    // return 32
    // }

    // }

    /* TODO: write a better lookup function */

    /* to check if successor in a chord ring */
    public boolean isSuccessor(int successorKey, int targetKey) {
        return successorKey == targetKey || isWithinRing(targetKey, this.selfPeerID, successorKey);
    }

    public boolean isWithinRing(int a, int start, int end) {

        if (start < end) {
            return a > start && a <= end;
        } else {
            return a > start || a <= end;
        }
    }

    public boolean isWithinRing(long a, long start, long end) {

        if (start < end) {
            return a > start && a <= end;
        } else {
            return a > start || a <= end;
        }
    }

    public void putEntry(int index, Entry entry) {
        table.add(index, entry);
    }

    private long calculateRingPosition(int index, int nodeID) {
        /* calculating nodeID + 2^i */
        long result = (nodeID + (long) Math.pow(2, index)) % (long) Math.pow(2, FT_ROWS);
        // int result = (nodeID + (1 << index)) % (1 << FT_ROWS);
        // System.out.println((1 + (1 << index)) % (1 << FT_ROWS));
        return result;
    }

    public void initialize() {
        for (int i = 0; i < FT_ROWS; i++) {
            Entry entry = new Entry(calculateRingPosition(i, selfPeerID), selfAddress, selfPort);
            table.add(entry);
        }

        this.predecessor = new Entry(-1, selfAddress, selfPort);
        this.successor = new Entry(getSuccessorStart(), selfAddress, selfPort);

        print();
    }

    public Entry getSuccessor() {
        return this.successor;
    }

    public Entry getPredecessor() {
        return predecessor;
    }

    public long getSuccessorStart() {
        return calculateRingPosition(0, selfPeerID);
    }

    public synchronized void updateSuccessor(String ipAddress, int port) {
        this.successor.updateEntry(ipAddress, port);
        // this.table.set(0, successor);
        updateTableWithPeer(this.successor);
        System.out.println("Successor updated to " + successor.getEntryString());

    }

    public synchronized void updatePredecessor(String ipAddress, int port) {
        this.predecessor.updateEntry(ipAddress, port);
        updateTableWithPeer(predecessor);
        System.out.println("Pred updated to " + predecessor.getEntryString());
    }

    private void updateTableWithPeer(Entry newPeer) {
        System.out.println("Before:");
        print();
        for (int i = 0; i < FT_ROWS; i++) {
            /*
             * this means, if the given peer lies between the start and node id of
             * any table entry, we have to update it
             */
            Entry tempEntry = table.get(i);
            boolean check = newPeer.getHashCode() == tempEntry.getRingPosition()
                    || isWithinRing(newPeer.getHashCode(), tempEntry.getRingPosition(),
                            tempEntry.getHashCode());
            if (check) {
                table.get(i).updateEntry(newPeer.getAddress(), newPeer.getPort());

            }
        }
        System.out.println("After:");
        print();
    }

    /* TODO: implement fix fingers */
    public void fixFingers() {
        /*
         * iterate through the finger table
         * calculate ring position i.e. the start value
         * check
         * send find successor request for this entry if current nodes id doesn't fall
         * between start and entry identifier
         * and update the node
         */

        /*
         * at regular intervals
         * find the successor of a random start value in finger table
         * 
         */
    }

    public void print() {
        System.out.println("Start   IPAddress   Port");
        for (Entry entry : table) {
            System.out.println(entry.getRingPosition() + "  " + entry.getAddress() + "  " + entry.getPort());
        }
    }

    /*
     * call this function only after checking current node is the successor of
     * lookup key k
     */
    public Entry lookup(int lookupId) {
        // first p >= k
        // q = FTp[j] ≤ k < FTp[j+1]
        // q = FTp[1] when p < k < FTp[1]
        if (isWithinRing(lookupId, selfPeerID, table.get(0).getHashCode())) {
            return table.get(0);
        } else {

            for (int i = 0; i < FT_ROWS - 1; i++) {
                Entry entry = table.get(i);
                if ((entry.getHashCode() <= lookupId) && (entry.getHashCode() < table.get(i + 1).getHashCode())) {
                    return entry;
                }

            }
            return table.get(table.size() - 1);
        }

    }

}
