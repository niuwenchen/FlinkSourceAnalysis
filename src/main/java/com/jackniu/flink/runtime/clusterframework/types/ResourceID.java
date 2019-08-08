package com.jackniu.flink.runtime.clusterframework.types;

import com.jackniu.flink.util.AbstractID;
import com.jackniu.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Created by JackNiu on 2019/7/7.
 */
public  final class ResourceID implements ResourceIDRetrievable, Serializable {
    private static final long serialVersionUID = 42L;

    private final String resourceId;

    public ResourceID(String resourceId) {
        Preconditions.checkNotNull(resourceId, "ResourceID must not be null");
        this.resourceId = resourceId;
    }

    /**
     * Gets the Resource Id as string
     * @return Stringified version of the ResourceID
     */
    public final String getResourceIdString() {
        return resourceId;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || o.getClass() != getClass()) {
            return false;
        } else {
            return resourceId.equals(((ResourceID) o).resourceId);
        }
    }

    @Override
    public final int hashCode() {
        return resourceId.hashCode();
    }

    @Override
    public String toString() {
        return resourceId;
    }

    /**
     * A ResourceID can always retrieve a ResourceID.
     * @return This instance.
     */
    @Override
    public ResourceID getResourceID() {
        return this;
    }

    /**
     * Generate a random resource id.
     * @return A random resource id.
     */
    public static ResourceID generate() {
        return new ResourceID(new AbstractID().toString());
    }
}
