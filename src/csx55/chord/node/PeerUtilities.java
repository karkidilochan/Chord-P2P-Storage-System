package csx55.chord.node;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import csx55.chord.tcp.TCPConnection;
import csx55.chord.utils.Entry;
import csx55.chord.wireformats.DownloadRequest;
import csx55.chord.wireformats.FileTransfer;
import csx55.chord.wireformats.FileTransferResponse;
import csx55.chord.wireformats.FindSuccessorTypes;
import csx55.chord.wireformats.GetPredecessor;
import csx55.chord.wireformats.IdentifiedSuccessor;
import csx55.chord.wireformats.Protocol;
import csx55.chord.wireformats.RequestSuccessor;

public class PeerUtilities {

    public static void handleDownloadRequest(DownloadRequest message, Peer peer, TCPConnection connection) {
        try {

            /* payload of message contains file name */
            String fileName = message.getFileName();
            FileTransfer request = new FileTransfer(false, fileName,
                    Files.readAllBytes(Paths.get("/tmp", String.valueOf(peer.getPeerID()), fileName)));
            connection.getTCPSenderThread().sendData(request.getBytes());
            connection.start();
        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred handling download request and sending file to peer: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void handleFileDownload(String fileName, Peer peer, FingerTable fingerTable) {
        /*
         * check if you are responsible for the file key
         * if not send find successor request to appropriate predecessor
         */
        int fileKey = Math.abs(fileName.hashCode());

        File uploadDirectory = new File("/tmp/" + peer.getPeerID());

        try {
            if (fingerTable.isWithinRing(fileKey, fingerTable.getPredecessor().getHashCode(), peer.getPeerID())) {
                if (!uploadDirectory.exists()) {
                    System.out.println("Error occurred while trying to read file, file doesn't exist.");
                    return;
                }
                File currentDirectory = new File(".");
                byte[] filePayload = Files
                        .readAllBytes(Paths.get("/tmp", String.valueOf(peer.getPeerID()), fileName));
                Files.write(Paths.get(currentDirectory.getAbsolutePath(), fileName), filePayload);
                System.out.println("Successfully downloaded requested file to current working directory.");
            } else {
                Entry lookupResult = fingerTable.lookup(fileKey);

                /* get its successor */
                Socket socketToPred = new Socket(lookupResult.getAddress(),
                        lookupResult.getPort());
                TCPConnection connectionToPred = new TCPConnection(peer, socketToPred);

                RequestSuccessor request = new RequestSuccessor(
                        FindSuccessorTypes.FILE_UPLOAD, fileName, fileKey, peer.getIPAddress(), peer.getPort());
                request.addPeerToHops(peer.getPeerID());
                connectionToPred.getTCPSenderThread().sendData(request.getBytes());
                connectionToPred.start();
            }
        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred while handling file download: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void handleFileUpload(String filePath, Peer peer, FingerTable fingerTable) {
        File uploadFile = new File(filePath);

        String filename = uploadFile.getName();
        int fileKey = Math.abs(filename.hashCode());

        /*
         * Workflow:
         * get 32-bit key for filename
         * perform lookup to find the successor(k)
         * forward a file upload request until the correct successor is found (same as
         * the node joining process)
         * the correct peer handles the upload request and stores it in /tmp/<peerID>/.
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
                File fileToUpload = new File(filePath);
                writeFile(peer.getPeerID(), fileToUpload.getName(), Files.readAllBytes(fileToUpload.toPath()),
                        fingerTable);
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

                RequestSuccessor message = new RequestSuccessor(
                        FindSuccessorTypes.FILE_UPLOAD, filePath, fileKey, peer.getIPAddress(), peer.getPort());
                message.addPeerToHops(peer.getPeerID());
                connectionToPred.getTCPSenderThread().sendData(message.getBytes());
                connectionToPred.start();

            }

        } catch (IOException | InterruptedException e) {
            System.out.println(
                    "Error occurred while handling file upload" + e.getMessage());
            e.printStackTrace();

        }
    }

    public static void sendFileToPeer(IdentifiedSuccessor message, Peer peer) {

        try {
            /*
             * read the file from the file path in the payload
             * then send transfer it to the identified peer
             */
            Socket socket = new Socket(message.getIPAddress(),
                    message.getPort());
            TCPConnection connection = new TCPConnection(peer, socket);

            /* payload of message contains file path */
            File fileToUpload = new File(message.getPayload());
            FileTransfer request = new FileTransfer(true, fileToUpload.getName(),
                    Files.readAllBytes(fileToUpload.toPath()));
            connection.getTCPSenderThread().sendData(request.getBytes());
            connection.start();
        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred while sending file to peer: " + e.getMessage());
            e.printStackTrace();
        }

    }

    public static void sendFileReceived(FileTransfer message, TCPConnection connection, Peer peer,
            FingerTable fingerTable) {
        /* deserialize the file and put it in the //tmp/peerid directory */
        byte status;
        String response;
        boolean isSuccessful;
        try {
            if (message.checkIfUpload()) {
                isSuccessful = writeFile(peer.getPeerID(), message.getFileName(), message.getFilePayload(),
                        fingerTable);
            } else {
                try {
                    /*
                     * TODO: check if file doesn't exist and send a error response to requesting
                     * peer
                     */
                    File currentDirectory = new File(".");
                    Files.write(Paths.get(currentDirectory.getAbsolutePath(), message.getFileName()),
                            message.getFilePayload());
                    isSuccessful = true;
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    e.printStackTrace();
                    isSuccessful = false;
                }

            }

            if (isSuccessful) {
                status = Protocol.SUCCESS;
                response = "File transfer was successful.";
            } else {
                status = Protocol.FAILURE;
                response = "File transfer failed.";
            }

            FileTransferResponse request = new FileTransferResponse(status, response);
            connection.getTCPSenderThread().sendData(request.getBytes());
        } catch (IOException | InterruptedException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

    }

    public static void sendDownloadRequest(IdentifiedSuccessor message, Peer peer) {

        try {
            Socket socket = new Socket(message.getIPAddress(),
                    message.getPort());
            TCPConnection connectionToPeer = new TCPConnection(peer, socket);

            DownloadRequest request = new DownloadRequest(message.getPayload());
            connectionToPeer.getTCPSenderThread().sendData(request.getBytes());
            connectionToPeer.start();
        } catch (IOException | InterruptedException e) {
            System.out.println("Error sending download request: " + e.getMessage());
            e.printStackTrace();
        }

    }

    /* call this function when current peer is the successor of the filekey */
    public static boolean writeFile(int nodeID, String fileName, byte[] filePayload, FingerTable fingerTable) {
        /* TODO: you are sending files through sockets */
        /* create /tmp/<peerID> if it doesn't exist */

        String uploadPath = "/tmp/" + nodeID;
        File uploadDirectory = new File(uploadPath);

        if (!uploadDirectory.exists()) {
            System.out.println("Upload directory doesn't exist. Creating...");
            uploadDirectory.mkdirs();
        }

        try {
            Path filePath = Paths.get("/tmp", String.valueOf(nodeID), fileName);
            if (!Files.exists(filePath)) {
                // Create a new file
                Files.createFile(filePath);
            }
            Files.write(filePath, filePayload);
            System.out.println("Successfully uploaded file " + fileName + " at: " + filePath.toAbsolutePath());
        } catch (IOException e) {
            System.out.println("Error occurred while trying to upload file: " +
                    e.getMessage());
            e.printStackTrace();
            return false;
        }

        fingerTable.fileIndex.put(fileName, Math.abs(fileName.hashCode()));

        return true;

    }

    public static void handleFileTransferResponse(FileTransferResponse message) {
        System.out.println("Received file transfer response from the peer: " + message.toString());
    }

    public static void joinNetwork(FingerTable fingerTable, IdentifiedSuccessor message, Peer peer) {
        /*
         * after getting your successor
         * contact successor
         * and contact your successor's predecessor
         */
        // update finger table and successor
        fingerTable.updateSuccessor(message.getIPAddress(), message.getPort());

        // query pred of this successor and make it your predecessor
        // also notify your successor you are its pred

        try {
            Socket socketToSuccessor = new Socket(message.getIPAddress(), message.getPort());
            TCPConnection successorConnection = new TCPConnection(peer, socketToSuccessor);

            GetPredecessor request = new GetPredecessor(peer.getIPAddress(), peer.getPort());

            successorConnection.getTCPSenderThread().sendData(request.getBytes());
            successorConnection.start();
        } catch (IOException | InterruptedException e) {
            System.out.println("Error occurred while requesting predecessor info." + e.getMessage());
            e.printStackTrace();
        }
    }

}
