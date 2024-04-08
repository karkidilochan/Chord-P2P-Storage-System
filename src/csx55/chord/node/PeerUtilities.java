package csx55.chord.node;

import java.io.IOException;
import java.net.Socket;

import csx55.chord.tcp.TCPConnection;
import csx55.chord.wireformats.GetPredecessor;
import csx55.chord.wireformats.IdentifiedSuccessor;
import csx55.chord.wireformats.Protocol;

public class PeerUtilities {

    public static void joinNetwork(FingerTable fingerTable, IdentifiedSuccessor message, Peer peer) {
        /*
         * after getting your sucessor
         * contact successor
         * and contact your sucessor's predecessor
         */
        // update finger table and successor
        fingerTable.updateSuccessor(message.getIPAddress(), message.getPort());

        // query pred of this successor and make it your predecessor
        // also notify your successor you are its pred

        try {
            Socket socketToSuccessor = new Socket(message.getIPAddress(), message.getPort());
            TCPConnection successorConnection = new TCPConnection(peer, socketToSuccessor);

            GetPredecessor request = new GetPredecessor(Protocol.GET_PREDECESSOR, peer.getIPAddress(), peer.getPort());

            successorConnection.getTCPSenderThread().sendData(request.getBytes());
            successorConnection.start();
        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred while requesting predecessor info." + e.getMessage());
            e.printStackTrace();
        }
    }

}
