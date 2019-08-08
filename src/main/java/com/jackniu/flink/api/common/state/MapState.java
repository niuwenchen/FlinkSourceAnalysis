package com.jackniu.flink.api.common.state;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by JackNiu on 2019/7/7.
 */
public interface   MapState<UK, UV> extends State {

    /**
     * Returns the current value associated with the given key.
     *
     * @param key The key of the mapping
     * @return The value of the mapping with the given key
     *
     * @throws Exception Thrown if the system cannot access the state.
     */
    UV get(UK key) throws Exception;

    /**
     * Associates a new value with the given key.
     *
     * @param key The key of the mapping
     * @param value The new value of the mapping
     *
     * @throws Exception Thrown if the system cannot access the state.
     */
    void put(UK key, UV value) throws Exception;

    /**
     * Copies all of the mappings from the given map into the state.
     *
     * @param map The mappings to be stored in this state
     *
     * @throws Exception Thrown if the system cannot access the state.
     */
    void putAll(Map<UK, UV> map) throws Exception;

    /**
     * Deletes the mapping of the given key.
     *
     * @param key The key of the mapping
     *
     * @throws Exception Thrown if the system cannot access the state.
     */
    void remove(UK key) throws Exception;

    /**
     * Returns whether there exists the given mapping.
     *
     * @param key The key of the mapping
     * @return True if there exists a mapping whose key equals to the given key
     *
     * @throws Exception Thrown if the system cannot access the state.
     */
    boolean contains(UK key) throws Exception;

    /**
     * Returns all the mappings in the state.
     *
     * @return An iterable view of all the key-value pairs in the state.
     *
     * @throws Exception Thrown if the system cannot access the state.
     */
    Iterable<Map.Entry<UK, UV>> entries() throws Exception;

    /**
     * Returns all the keys in the state.
     *
     * @return An iterable view of all the keys in the state.
     *
     * @throws Exception Thrown if the system cannot access the state.
     */
    Iterable<UK> keys() throws Exception;

    /**
     * Returns all the values in the state.
     *
     * @return An iterable view of all the values in the state.
     *
     * @throws Exception Thrown if the system cannot access the state.
     */
    Iterable<UV> values() throws Exception;

    /**
     * Iterates over all the mappings in the state.
     *
     * @return An iterator over all the mappings in the state
     *
     * @throws Exception Thrown if the system cannot access the state.
     */
    Iterator<Map.Entry<UK, UV>> iterator() throws Exception;
}

