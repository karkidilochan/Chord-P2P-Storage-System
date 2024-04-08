package csx55.chord.node;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;

import csx55.chord.tcp.TCPConnection;
import csx55.chord.utils.Entry;
import csx55.chord.wireformats.DownloadRequest;
import csx55.chord.wireformats.FindSuccessorTypes;
import csx55.chord.wireformats.IdentifiedSuccessor;
import csx55.chord.wireformats.Protocol;
import csx55.chord.wireformats.RequestSuccessor;

public class PeerFileUtils {

    public static void handleFileUpload(String filePath, Peer peer, FingerTable fingerTable) {
        File uploadFile = new File(filePath);

        String filename = uploadFile.getName();
        int fileKey = filename.hashCode();

        /*
         * Workflow:
         * get 32-bit key for filename
         * perform lookup to find the successor(k)
         * forward a file upload request until the correct successor is found (same as
         * the node joining process)
         * the correct peer handles the upload request and stores it in tmp/<peerID>/.
         * 
         */

        try {
            /*
             * first look for the successor of lookup id in peer table
             * if found, send it back to the original peer
             * else
             * forward the successor request to node with closes succeeding id
             */
            if (fingerTable.isWithinRing(fileKey, fingerTable.getPredecessor().getHashCode(), peer.getPeerID())) {
                /* this node is the successor of k, so it handles the upload */
                uploadFile(peer.getPeerID(), filePath);
            } else {
                /*
                 * now forward the find successor request to closest succeeding id
                 * for this find the closest predecessor and ping to get its successor
                 */

                // call fingertable lookup function to find the predecessor

                Entry lookupResult = fingerTable.lookup(fileKey);

                /* get its successor */
                Socket socketToPred = new Socket(lookupResult.getAddress(),
                        lookupResult.getPort());
                TCPConnection connectionToPred = new TCPConnection(peer, socketToPred);

                RequestSuccessor message = new RequestSuccessor(Protocol.REQUEST_SUCCESSOR,
                        FindSuccessorTypes.FILE_UPLOAD, filePath, fileKey, peer.getIPAddress(), peer.getPort());
                connectionToPred.getTCPSenderThread().sendData(message.getBytes());
                connectionToPred.start();

            }

        } catch (IOException | InterruptedException e) {
            System.out.println(
                    "Error occured while handling file upload" + e.getMessage());
            e.printStackTrace();

        }
    }

    public static void sendFileToPeer(IdentifiedSuccessor message, Peer peer) {
        /*
         * read the file from the file path in the payload
         * then send transfer it to the identified peer
         */
    }

    public static void sendFileReceived() {
        /* deserialize the file and put it in the /tmp/peerid directory */
    }

    public static void sendDownloadRequest(IdentifiedSuccessor message, Peer peer) {
        // Socket socket = new Socket(message.getIPAddress(),
        // message.getPort());
        // TCPConnection connectionToPred = new TCPConnection(peer, socket);

        // DownloadRequest request = new DownloadRequest(Protocol.REQUEST_SUCCESSOR,
        // FindSuccessorTypes.FILE_UPLOAD, filePath, fileKey, peer.getIPAddress(),
        // peer.getPort());
        // connectionToPred.getTCPSenderThread().sendData(message.getBytes());
        // connectionToPred.start();
    }

    /* call this function when current peer is the successor of the filekey */
    public static void uploadFile(int nodeID, String filePath) {
        /* TODO: you are sending files through sockets */
        /* create /tmp/<peerID> if it doesn't exist */

        /*
         * String uploadPath = "/tmp/" + nodeID;
         * File uploadDirectory = new File(uploadPath);
         * 
         * if (uploadDirectory.exists()) {
         * System.out.println("Upload directory doesn't exist. Creating...");
         * uploadDirectory.mkdirs();
         * }
         * 
         * try {
         * Files.copy(Paths.get(filePath), Paths.get(uploadPath));
         * System.out.println("Succesfully uploaded file" + filePath);
         * } catch (IOException e) {
         * System.out.println("Error occurred while trying to upload file: " +
         * e.getMessage());
         * e.printStackTrace();
         * }
         */

    }

}
