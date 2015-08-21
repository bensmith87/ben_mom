package ben.mom.client;

import org.jetbrains.annotations.NotNull;

/**
 * Event Queue Interface.
 */
public interface IEventQueue {

    /**
     * Invoke a runnable on the event queue.
     * @param runnable the runnable to invoke
     */
    void invoke(@NotNull Runnable runnable);
}
