package edu.uci.ics.textdb.perftest.chinesenews;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.lucene.analysis.Analyzer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.plan.Plan;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.dataflow.sink.IndexSink;
import edu.uci.ics.textdb.dataflow.source.FileSourceOperator;

public class ChineseNews {
    
    public static final String URL = "url";
    public static final String TITLE = "title";
    public static final String CONTENT = "content";
    
    public static final Attribute URL_ATTR = new Attribute(URL, FieldType.STRING);
    public static final Attribute TITLE_ATTR = new Attribute(TITLE, FieldType.TEXT);
    public static final Attribute CONTENT_ATTR = new Attribute(CONTENT, FieldType.TEXT);
    
    public static final Schema CHINESE_NEWS_SCHEMA = new Schema(
            URL_ATTR, TITLE_ATTR, CONTENT_ATTR);
    
    public static List<ITuple> getChineseNewsTuples(String filePath) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File(filePath));
        StringBuilder sb = new StringBuilder();
        ArrayList<ITuple> newsTupleList = new ArrayList<>();
        
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            sb.append(line);
            if (line.trim().endsWith("</doc>")) {
                ITuple newsTuple = convertChineseNewsTuple(sb.toString());
                if (newsTuple != null) {
                    newsTupleList.add(newsTuple);
                }
                sb.setLength(0);
            }
        }
        
        scanner.close();
        return newsTupleList;     
    }
    
    public static ITuple convertChineseNewsTuple(String docContent) {
        try {
            JSONObject fileJson = org.json.XML.toJSONObject(docContent);            
            ITuple tuple = new DataTuple(CHINESE_NEWS_SCHEMA,
                    new StringField(fileJson.getString("url")),
                    new TextField(fileJson.getString("contentTitle")), 
                    new TextField(fileJson.getString("content")));
            
            return tuple;
        } catch (JSONException e) {
            return null;
        }
    }
    
    public Plan getChineseNewsIndexPlan(String filePath, IDataStore dataStore, Analyzer luceneAnalyzer) {
        List<ITuple> chineseNewsTuples = getChineseNewsTuples(filePath);
        
        IndexSink chineseNewsSink = new IndexSink(dataStore.getDataDirectory(), dataStore.getSchema(), luceneAnalyzer, false);
        ISourceOperator fileSourceOperator = new TupleStreamSourceOperator(chineseNewsTuples);
                dataStore.getSchema());
        chineseNewsSink.setInputOperator(fileSourceOperator);

        Plan writeIndexPlan = new Plan(chineseNewsSink);

        return writeIndexPlan;
    }
    
    public static void main(String[] args) throws Exception {
        getChineseNewsTuples("./sample-data-files/news_sohusite_xml.smarty.txt");
    }
    

}
