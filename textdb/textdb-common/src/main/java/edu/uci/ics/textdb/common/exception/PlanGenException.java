package edu.uci.ics.textdb.common.exception;

public class PlanGenException extends Exception {

    private static final long serialVersionUID = -5670429837390859597L;

    public PlanGenException(String errorMessage, Throwable throwable) {
        super(errorMessage, throwable);
    }
    
    public PlanGenException(String errorMessage) {
        super(errorMessage);
    }

}
