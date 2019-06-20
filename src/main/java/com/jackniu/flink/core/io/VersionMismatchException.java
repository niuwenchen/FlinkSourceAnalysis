package com.jackniu.flink.core.io;

import java.io.IOException;

/**
 * Created by JackNiu on 2019/6/19.
 */
public class VersionMismatchException  extends IOException {
    private static final long serialVersionUID = 7024258967585372438L;

    public VersionMismatchException() {
    }

    public VersionMismatchException(String message) {
        super(message);
    }

    public VersionMismatchException(String message, Throwable cause) {
        super(message, cause);
    }

    public VersionMismatchException(Throwable cause) {
        super(cause);
    }
}
