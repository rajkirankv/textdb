package edu.uci.ics.textdb.dataflow.keywordmatch;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.utils.Utils;

/**
 * @author Zuozhi Wang
 * @author prakul
 *
 * This class handles creation of predicate for querying using Keyword Matcher
 */
public class KeywordPredicate implements IPredicate {

    private List<String> attributeNames;
    private String query;
    private Query luceneQuery;
    private ArrayList<String> queryTokenList;
    private HashSet<String> queryTokenSet;
    private ArrayList<String> queryTokensWithStopwords;
    private Analyzer luceneAnalyzer;
    private KeywordMatchingType keywordMatchingType;
    private String spanListName;

    /*
     * query refers to string of keywords to search for. For Ex. New york if
     * searched in TextField, we would consider both tokens New and York; if
     * searched in String field we search for Exact string.
     */
    public KeywordPredicate(String query, List<String> attributeNames, Analyzer luceneAnalyzer,
            KeywordMatchingType keywordMatchingType) {
        this(query, attributeNames, luceneAnalyzer, keywordMatchingType, null);
    }
    
    public KeywordPredicate(String query, List<String> attributeNames, Analyzer luceneAnalyzer,
            KeywordMatchingType keywordMatchingType, String spanListName) {
        this.query = query;
        this.queryTokenList = Utils.tokenizeQuery(luceneAnalyzer, query);
        this.queryTokenSet = new HashSet<>(this.queryTokenList);
        this.queryTokensWithStopwords = Utils.tokenizeQueryWithStopwords(query);

        this.attributeNames = attributeNames;
        this.keywordMatchingType = keywordMatchingType;

        this.luceneAnalyzer = luceneAnalyzer;
        this.spanListName = spanListName;
    }

    public KeywordMatchingType getKeywordMatchingType() {
        return keywordMatchingType;
    }

    public String getQuery() {
        return query;
    }

    public List<String> getAttributeNames() {
        return attributeNames;
    }

    public Query getQueryObject() {
        return this.luceneQuery;
    }

    public ArrayList<String> getQueryTokenList() {
        return this.queryTokenList;
    }

    public HashSet<String> getQueryTokenSet() {
        return this.queryTokenSet;
    }

    public ArrayList<String> getQueryTokensWithStopwords() {
        return this.queryTokensWithStopwords;
    }

    public Analyzer getLuceneAnalyzer() {
        return luceneAnalyzer;
    }
    
    public String getSpanListName() {
        return this.spanListName;
    }

}
