package org.bluedb.api.exceptions;

public class InvalidKeyTypeException extends Exception{

    public InvalidKeyTypeException(String message, Throwable cause) {
        super(message, cause);
    }
    public InvalidKeyTypeException(String message) {
        super(message);
    }
}
