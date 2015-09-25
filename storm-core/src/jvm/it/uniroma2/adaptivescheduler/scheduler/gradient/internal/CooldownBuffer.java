package it.uniroma2.adaptivescheduler.scheduler.gradient.internal;

import java.util.*;

/**
 * Created by Gabriele Scolastri on 15/09/15.
 */
public class CooldownBuffer<T> {

    private static final Integer MIN_COOLDOWN_TIMER = 1;

    private Map<T, Integer> activeCooldowns;
    private Integer defaultCooldownTimeout = MIN_COOLDOWN_TIMER;

    public CooldownBuffer(int defaultCooldownTimeout)
    {
        this.defaultCooldownTimeout = (defaultCooldownTimeout > 0 ? defaultCooldownTimeout : MIN_COOLDOWN_TIMER);
        this.activeCooldowns = new HashMap<T, Integer>();
    }

    public CooldownBuffer()
    {
        this.defaultCooldownTimeout = MIN_COOLDOWN_TIMER;
        this.activeCooldowns = new HashMap<T, Integer>();
    }

    /**
     * Sets the specified item in cooldown for the default time.
     *
     * @param item
     */
    public void set(T item)
    {
        activeCooldowns.put(item, defaultCooldownTimeout);
    }

    /**
     * Sets the specified item in cooldown for the specified cooldownTimer time.
     *
     * @param item: item to put in cooldown
     * @param cooldownTimer: cooldown time
     */

    public void set(T item, Integer cooldownTimer)
    {
        activeCooldowns.put(item, (cooldownTimer>0 ? cooldownTimer: MIN_COOLDOWN_TIMER));
    }

    /**
     * Advances the cooldown "timer" by one unit. Items whose cooldown
     * reaches 0 are removed automatically.
     */
    public void tick()
    {
        Integer cooldownCounter;
        for(T item : activeCooldowns.keySet())
        {
            cooldownCounter = activeCooldowns.get(item);

            if(cooldownCounter > 0)
                activeCooldowns.put(item, cooldownCounter - 1);
            else
                activeCooldowns.remove(item);
        }
    }

    /**
     * Removes the specified item from cooldown.
     *
     * @param item: item whose cooldown has to be removed
     */
    public void remove(T item)
    {
        activeCooldowns.remove(item);
    }

    /**
     * This function removes the specified items cooldowns
     *
     * @param items: list of cooldown items to remove
     */
    public void remove(Collection<T> items)
    {
        for(T item : items)
            activeCooldowns.remove(item);
    }


    /**
     * This function removes items in cooldown which are NOT
     * within the list passed as parameter.
     *
     * (Sometimes, you know the items you still need to monitor
     * and want to cut out the others.)
     *
     * @param items: items whose cooldown must be preserved (others are cancelled)
     */
    public void removeOtherThan(Collection<T> items)
    {
        Set<T> toRemove = new HashSet<T>(activeCooldowns.keySet());
        for(T item : items)
            toRemove.remove(item);

        for(T item : toRemove)
            activeCooldowns.remove(item);
    }

    /**
     * Checks whether the specified item is still cooling down or not (if never put
     * in cooldown, it will result as not in cooldown).
     *
     * @param item: item whose cooldown state must be verified
     * @return the cooldown state of the item, i.e. if the item is still cooling down
     */

    public boolean isCoolingDown(T item)
    {
        return activeCooldowns.containsKey(item);
    }
}
