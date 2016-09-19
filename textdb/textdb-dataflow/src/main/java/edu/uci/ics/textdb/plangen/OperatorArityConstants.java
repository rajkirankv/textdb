package edu.uci.ics.textdb.plangen;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OperatorArityConstants {
    
    public class OperatorArity {
        public int inputArity;
        public int outputArity;
        
        public OperatorArity(int inputArity, int outputArity) {
            this.inputArity = inputArity;
            this.outputArity = outputArity;
        }     
    }
    
    
    public static Map<String, OperatorArity> operatorArityMap = new HashMap<>();
    static {
        List<String> singleInputOperators = new ArrayList<>();
    }
    

}
