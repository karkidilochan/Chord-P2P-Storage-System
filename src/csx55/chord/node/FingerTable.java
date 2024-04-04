package csx55.chord.node;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import csx55.chord.utils.Entry;

public class FingerTable {

    private List<Entry> table = new ArrayList<>();

    private int selfPeerID;

    private String selfAddress;

    private int selfPort;

    private final int FT_ROWS = 32;

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

    public void putEntry(int index, Entry entry) {
        table.add(index, entry);
    }

    private int calculateRingPosition(int index, int nodeID) {
        /* calculating nodeID + 2^i */
        return (nodeID + (1 << (index))) % (1 << 32);
    }

    public void initialize() {
        for (int i = 0; i <= FT_ROWS; i++) {
            Entry entry = Entry(calculateRingPosition(i, selfPeerID), selfAddress, selfPort);
            table.add(entry);
        }
    }

}
