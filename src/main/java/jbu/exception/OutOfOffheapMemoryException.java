package jbu.exception;

public class OutOfOffheapMemoryException extends RuntimeException {

    public OutOfOffheapMemoryException(String message) {
        super(message);
    }

    public OutOfOffheapMemoryException(String message, Throwable cause) {
        super(message, cause);
    }
}
