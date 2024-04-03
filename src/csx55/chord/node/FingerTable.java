package csx55.chord.node;

import java.util.HashMap;
import java.util.Map;

public class FingerTable {

    private Map<Integer, String> table = new HashMap<>();

    private int selfPeerID;

    public FingerTable(int selfPeerID) {
        this.selfPeerID = selfPeerID;
    }

    public Map<Integer, String> getFingerTable() {
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

}
