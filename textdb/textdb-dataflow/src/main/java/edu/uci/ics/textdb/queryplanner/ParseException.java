package edu.uci.ics.textdb.queryplanner;

public class ParseException extends Exception {

    private static final long serialVersionUID = -5670429837390859597L;

    public ParseException(String errorMessage, Throwable throwable) {
        super(errorMessage, throwable);
    }
    
    public ParseException(String errorMessage) {
        super(errorMessage);
    }

}
