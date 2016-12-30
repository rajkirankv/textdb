package edu.uci.ics.textdb.perftest.chinesenews;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.MatchAllDocsQuery;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.plan.Plan;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.engine.Engine;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.reader.DataReader;

public class ChineseNewsExtractor {
    
    public static final String CHINESE_NEWS_DATA_PATH = 
            "./sample-data-files/news_sohusite_xml.smarty.txt";
    
    public static final DataStore CHINESE_NEWS_DATA_STORE = 
            new DataStore("../index/chinesenews/", ChineseNewsConstants.CHINESE_NEWS_SCHEMA);
    
    public static void main(String[] args) throws Exception {
        // write index
        Plan writeChineseNewsIndexPlan = ChineseNewsConstants.getWriteChineseNewsIndexPlan(
                CHINESE_NEWS_DATA_PATH,
                CHINESE_NEWS_DATA_STORE,
                LuceneAnalyzerConstants.getLuceneAnalyzer("smartchinese"));
        
        Engine.getEngine().evaluate(writeChineseNewsIndexPlan);
        
        // read index
        DataReaderPredicate dataReaderPredicate = new DataReaderPredicate(new MatchAllDocsQuery(), CHINESE_NEWS_DATA_STORE);
        dataReaderPredicate.setIsPayloadAdded(true);
        DataReader dataReader = new DataReader(dataReaderPredicate);
        
        List<ITuple> results = new ArrayList<>();
        ITuple tuple;
        
        dataReader.open();
        while ((tuple = dataReader.getNextTuple()) != null) {
            results.add(tuple);
        }
        dataReader.close();
        
        System.out.println(Utils.getTupleListString(results));

    }

}
