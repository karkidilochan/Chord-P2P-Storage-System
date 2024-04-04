package csx55.chord.wireformats;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * WireFormatGenerator is a singleton class responsible for generating messages
 * of different types.
 */
public class WireFormatGenerator {
    private static final WireFormatGenerator messageGenerator = new WireFormatGenerator();

    // private constructor to prevent instantiation
    private WireFormatGenerator() {
    }

    /**
     * Returns the singleton instance of WireFormatGenerator.
     * 
     * @return The WireFormatGenerator instance.
     */
    public static WireFormatGenerator getInstance() {
        return messageGenerator;
    }

    /**
     * Creates a wireformat message from the received marshaled bytes.
     * 
     * @param marshalledData The marshaled bytes representing the event.
     * @return The Event object created from the marshaled bytes.
     */
    /* Create message wireformats from received marshalled bytes */
    public Event createMessage(byte[] marshalledData) throws IOException, ClassNotFoundException {
        int type = ByteBuffer.wrap(marshalledData).getInt();
        switch (type) {
            case Protocol.REGISTER_REQUEST:
                return new Register(marshalledData);

            case Protocol.REGISTER_RESPONSE:
                return new RegisterResponse(marshalledData);

            case Protocol.DEREGISTER_REQUEST:
                return new Register(marshalledData);

            case Protocol.MESSAGING_NODES_LIST:
                return new ComputeNodesList(marshalledData);

            case Protocol.SETUP_CHORD:
                return new SetupChord(marshalledData);

            case Protocol.REQUEST_SUCCESSOR:
                return new RequestSuccessor(marshalledData);

            case Protocol.SUCCESSOR_IDENTIFIED:
                return new IdentifiedSuccessor(marshalledData);

            case Protocol.NOTIFY_SUCCESSOR:
                return new NotifyYourSuccessor(marshalledData);

            case Protocol.NOTIFY_PREDECESSOR:
                return new NotifyYourPredecessor(marshalledData);

            case Protocol.GET_PREDECESSOR:
                return new GetPredecessor(marshalledData);

            case Protocol.GET_PREDECESSOR_RESPONSE:
                return new GetPredecessorResponse(marshalledData);

            default:
                System.out.println("Error: WireFormat could not be generated. " + type);
                return null;
        }
    }
}
