package ben.mom.client;

import org.jetbrains.annotations.NotNull;

/**
 * Interface for a message processor.
 *
 * @param <T> the class of the message object that will be processed
 */
public interface IMessageProcessor<T> {

    /**
     * Process a message.
     * @param body the message body
     */
    void processMessage(@NotNull T body);
}
