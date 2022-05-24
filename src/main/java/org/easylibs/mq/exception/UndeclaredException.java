package org.easylibs.mq.exception;

public class UndeclaredException extends Exception {
    public UndeclaredException() {
        this("Declare dispatcher before using it.");
    }

    public UndeclaredException(String msg) {
        super(msg);
    }
}
