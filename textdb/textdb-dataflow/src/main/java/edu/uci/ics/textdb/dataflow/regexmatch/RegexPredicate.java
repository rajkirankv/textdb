package edu.uci.ics.textdb.dataflow.regexmatch;

import java.util.List;

import edu.uci.ics.textdb.api.common.IPredicate;


/**
 * This class is the predicate for Regex.
 * 
 * @author Zuozhi Wang
 * @author Shuying Lai
 *
 */
public class RegexPredicate implements IPredicate {

    private String regex;
    private List<String> attributeNames;  
    private String spanListName;


    public RegexPredicate(String regex, List<String> attributeNames) {
        this(regex, attributeNames, null);
    }
    
    public RegexPredicate(String regex, List<String> attributeNames, String spanListName) {
        this.regex = regex;
        this.attributeNames = attributeNames;
        this.spanListName = spanListName;
    }

    public String getRegex() {
        return regex;
    }

    public List<String> getAttributeNames() {
        return attributeNames;
    }
    
    public String getSpanListName() {
        return this.spanListName;
    }

}
