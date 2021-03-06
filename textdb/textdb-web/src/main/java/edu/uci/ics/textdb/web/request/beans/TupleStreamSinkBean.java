package edu.uci.ics.textdb.web.request.beans;

import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("TupleStreamSink")
public class TupleStreamSinkBean extends OperatorBean {
    // Properties regarding the projection operator will go here
    
    public TupleStreamSinkBean() {
    }
    
    public TupleStreamSinkBean(String operatorID, String operatorType, String attributes, String limit, String offset) {
        super(operatorID, operatorType, attributes, limit, offset);
    }
    
    @Override
    public HashMap<String, String> getOperatorProperties() {
        HashMap<String, String> operatorProperties = super.getOperatorProperties();
        if(operatorProperties == null)
            return null;
        return operatorProperties;
    }
    
    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (!(other instanceof TupleStreamSinkBean)) return false;
        TupleStreamSinkBean tupleStreamSinkBean = (TupleStreamSinkBean) other;
        return super.equals(tupleStreamSinkBean);
    }
}
