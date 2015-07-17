package ben.mom.client;

import java.io.Serializable;

/**
 * MOM Client Interface.
 * <p>
 *     The MOM Client is the client side part of the message broker, it allows client code to subscribe to messages and
 *     send messages.
 * </p>
 */
public interface IMomClient {

    /**
     * Subscribe to a particular message class.
     * @param <T> the class of the message object that will be processed
     * @param messageClass the class of the message to subscribe to
     * @param messageProcessor the processor that will handle the message
     */
    <T> void subscribe(Class<T> messageClass, IMessageProcessor<T> messageProcessor);

    /**
     * Send a message.
     * <p>
     *     If the object to be sent is a modified object that has already been sent it must be cloned otherwise the
     *     receiver on the other end will not get the modifications.
     * </p>
     * @param destination the destination
     * @param message the message to send
     */
    void sendMessage(String destination, Serializable message);
}
