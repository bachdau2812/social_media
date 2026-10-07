package com.dauducbach.clone.commons.vector;

/** A durable acknowledged state was lost or changed. Do not replay completed work automatically. */
public class VectorRepairRequiredException extends RuntimeException {
    public VectorRepairRequiredException(String message) { super(message); }
    public VectorRepairRequiredException(String message, Throwable cause) { super(message, cause); }
}
