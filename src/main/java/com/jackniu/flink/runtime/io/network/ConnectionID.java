package com.jackniu.flink.runtime.io.network;

import com.jackniu.flink.runtime.taskmanager.TaskManagerLocation;

import java.io.Serializable;
import java.net.InetSocketAddress;

import static com.jackniu.flink.util.Preconditions.checkArgument;
import static com.jackniu.flink.util.Preconditions.checkNotNull;

/**
 * Created by JackNiu on 2019/7/7.
 */
public class ConnectionID implements Serializable {

    private static final long serialVersionUID = -8068626194818666857L;

    private final InetSocketAddress address;

    private final int connectionIndex;

    public ConnectionID(TaskManagerLocation connectionInfo, int connectionIndex) {
        this(new InetSocketAddress(connectionInfo.address(), connectionInfo.dataPort()), connectionIndex);
    }

    public ConnectionID(InetSocketAddress address, int connectionIndex) {
        this.address = checkNotNull(address);
        checkArgument(connectionIndex >= 0);
        this.connectionIndex = connectionIndex;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public int getConnectionIndex() {
        return connectionIndex;
    }

    @Override
    public int hashCode() {
        return address.hashCode() + (31 * connectionIndex);
    }

    @Override
    public boolean equals(Object other) {
        if (other.getClass() != ConnectionID.class) {
            return false;
        }

        final ConnectionID ra = (ConnectionID) other;
        if (!ra.getAddress().equals(address) || ra.getConnectionIndex() != connectionIndex) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return address + " [" + connectionIndex + "]";
    }
}
