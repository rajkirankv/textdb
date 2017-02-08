package edu.uci.ics.textdb.perftest.sample;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.LuceneAnalyzerConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.Dictionary;
import edu.uci.ics.textdb.dataflow.common.DictionaryPredicate;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.dictionarymatcher.DictionaryMatcher;
import edu.uci.ics.textdb.dataflow.join.Join;
import edu.uci.ics.textdb.dataflow.join.SimilarityJoinPredicate;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcherSourceOperator;
import edu.uci.ics.textdb.dataflow.projection.ProjectionOperator;
import edu.uci.ics.textdb.dataflow.projection.ProjectionPredicate;
import edu.uci.ics.textdb.dataflow.sink.TupleStreamSink;
import edu.uci.ics.textdb.perftest.medline.MedlineIndexWriter;

public class SampleMedline {
    
    public static final String MEDLINE_TABLE = "abstract_10K";
    
    public static void main(String[] args) throws TextDBException {
        extractDiabetes();
    }
    
    public static Dictionary getDrugDictionary() {
        try {
            ArrayList<String> drugs = new ArrayList<>();
            File drugDictFile = new File("./sample-data-files/drugs_single.txt");
            Scanner scanner = new Scanner(drugDictFile);
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                drugs.add(line);
            }
            scanner.close();
            return new Dictionary(drugs);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public static List<String> getInnerOuterAttrs(List<String> attrs) {
        List<String> result = new ArrayList<>();
        for (String attr : attrs) {
            result.add("inner_" + attr);
        }
        for (String attr: attrs) {
            result.add("outer_" + attr);
        }
        return result;
    }
    
    public static void extractDiabetes() throws TextDBException {
        KeywordPredicate diabetesKeyword = new KeywordPredicate(
                "diabetes", 
                Arrays.asList(MedlineIndexWriter.ABSTRACT), 
                LuceneAnalyzerConstants.getLuceneAnalyzer(LuceneAnalyzerConstants.standardAnalyzerString()), 
                KeywordMatchingType.CONJUNCTION_INDEXBASED);
        
        KeywordMatcherSourceOperator keywordSourceDiabetes = new KeywordMatcherSourceOperator(
                diabetesKeyword, MEDLINE_TABLE);
        
        KeywordPredicate heartFailureKeyword = new KeywordPredicate(
                "heart failure",
                Arrays.asList(MedlineIndexWriter.ABSTRACT),
                LuceneAnalyzerConstants.getLuceneAnalyzer(LuceneAnalyzerConstants.standardAnalyzerString()), 
                KeywordMatchingType.CONJUNCTION_INDEXBASED);
        
        KeywordMatcherSourceOperator keywordSourceHeartFailure = new KeywordMatcherSourceOperator(
                heartFailureKeyword, MEDLINE_TABLE);
                
        ArrayList<String> attrNamesWithID = new ArrayList<>();
        attrNamesWithID.add(SchemaConstants._ID);
        attrNamesWithID.addAll(MedlineIndexWriter.SCHEMA_MEDLINE.getAttributeNames());
        attrNamesWithID.add(SchemaConstants.PAYLOAD);
        ProjectionPredicate projectOutSpan = new ProjectionPredicate(attrNamesWithID);
            
        ProjectionOperator project1 = new ProjectionOperator(projectOutSpan);
        ProjectionOperator project2 = new ProjectionOperator(projectOutSpan);
        
        DictionaryPredicate drugDict = new DictionaryPredicate(getDrugDictionary(), 
                Arrays.asList(MedlineIndexWriter.ABSTRACT), 
                LuceneAnalyzerConstants.getLuceneAnalyzer(LuceneAnalyzerConstants.standardAnalyzerString()),
                KeywordMatchingType.CONJUNCTION_INDEXBASED);
        
        DictionaryMatcher dictMatcher1 = new DictionaryMatcher(drugDict);
        DictionaryMatcher dictMatcher2 = new DictionaryMatcher(drugDict);
        
        SimilarityJoinPredicate simJoinPred = new SimilarityJoinPredicate(MedlineIndexWriter.ABSTRACT, 1.0);
        Join simJoin = new Join(simJoinPred);
        
        ProjectionOperator project3 = new ProjectionOperator(
                new ProjectionPredicate(getInnerOuterAttrs(Arrays.asList(
                        MedlineIndexWriter.PMID, MedlineIndexWriter.ARTICLE_TITLE, MedlineIndexWriter.ABSTRACT))));
        
        TupleStreamSink tupleStreamSink = new TupleStreamSink();
        
        
        project1.setInputOperator(keywordSourceDiabetes);
        project2.setInputOperator(keywordSourceHeartFailure);
        dictMatcher1.setInputOperator(project1);
        dictMatcher2.setInputOperator(project2);
        simJoin.setInnerInputOperator(dictMatcher1);
        simJoin.setOuterInputOperator(dictMatcher2);
        project3.setInputOperator(simJoin);
        tupleStreamSink.setInputOperator(project3);
        
        tupleStreamSink.open();
        List<ITuple> results = tupleStreamSink.collectAllTuples();
        tupleStreamSink.close();
        
        System.out.println(Utils.getTupleListString(results));
        
    }
    
    

}
