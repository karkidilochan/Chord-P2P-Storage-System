package csx55.utils;

import java.util.Random;
import java.net.ConnectException;

import csx55.chord.FingerTable;
import csx55.chord.Peer;
import csx55.tcp.TCPConnection;
import csx55.wireformats.FindSuccessorTypes;
import csx55.wireformats.RequestSuccessor;

import java.net.Socket;
import java.io.IOException;

public class FixFingers extends Thread {

    FingerTable fingerTable;
    Peer peer;
    boolean isAlive;
    Random random = new Random();

    public FixFingers(FingerTable fingerTable, Peer peer) {
        this.fingerTable = fingerTable;
        this.peer = peer;
        this.isAlive = true;
    }

    @Override
    public void run() {
        // isAlive = false;
        while (isAlive) {
            int i = random.nextInt(30) + 1;
            Entry entry = fingerTable.getTable().get(i);
            /*
             * send find successor request for the ith finger table entry
             * create a new type for this request FIX_FINGERS, it will have value of i as
             * payload
             */
            try {
                RequestSuccessor request = new RequestSuccessor(FindSuccessorTypes.FIX_FINGERS, String.valueOf(i),
                        entry.getRingPosition(), peer.getIPAddress(), peer.getPort());

                Socket socketToPeer = new Socket(entry.getAddress(), entry.getPort());
                TCPConnection connection = new TCPConnection(peer, socketToPeer);
                connection.getTCPSenderThread().sendData(request.getBytes());
                connection.start();
                Thread.sleep(5000);
            } catch (ConnectException e) {
                // do nothing
                System.out.println("No connection");
            } catch (IOException | InterruptedException e) {
                System.out.println(
                        "Error occurred while sending find successor for fix fingers routine: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public void stopRoutine() {
        isAlive = false;
    }
}
