package ben.mom.server;

import ben.mom.message.Message;

/**
 * Connection Listener Interface.
 * <p>
 * Connection listeners are notified whenever a message is received or the connection is dropped.
 */
public interface IConnectionListener {

    /**
     * The Connection has received a message.
     * @param message the message
     */
    void messageReceived(Message message);

    /**
     * The Connection has dropped.
     * @param clientName the name of the client that dropped connection
     */
    void connectionDropped(String clientName);
}
