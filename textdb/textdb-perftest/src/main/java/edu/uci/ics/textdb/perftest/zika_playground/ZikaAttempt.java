package edu.uci.ics.textdb.perftest.zika_playground;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IConnector;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.plan.Plan;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.IJoinPredicate;
import edu.uci.ics.textdb.dataflow.common.JoinDistancePredicate;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.connector.OneToNBroadcastConnector;
import edu.uci.ics.textdb.dataflow.join.Join;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpExtractor;
import edu.uci.ics.textdb.dataflow.nlpextrator.NlpPredicate;
import edu.uci.ics.textdb.dataflow.projection.ProjectionOperator;
import edu.uci.ics.textdb.dataflow.projection.ProjectionPredicate;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexMatcher;
import edu.uci.ics.textdb.dataflow.sink.FileSink;
import edu.uci.ics.textdb.dataflow.sink.IndexSink;
import edu.uci.ics.textdb.dataflow.source.IndexBasedSourceOperator;
import edu.uci.ics.textdb.dataflow.source.IterableSourceOperator;
import edu.uci.ics.textdb.engine.Engine;
import edu.uci.ics.textdb.perftest.utils.PerfTestUtils;
import edu.uci.ics.textdb.storage.DataStore;

public class ZikaAttempt {

    private static String standardIndexPath = "./index/testindex/promed/standard/";
    private static String trigramIndexPath = "./index/testindex/promed/trigram/";
    
    private static int idCounter = 0;
    
    public static void main(String[] args) throws Exception {
//         writePromedMailIndex();
        System.out.println("start");
//        extractPerson();
//        extractLocation();
        extractPersonLocationDate2();
        System.out.println("end");
    }


    public static ITuple parsePromedHTML(String content) throws Exception {
        Document parsedDocument = Jsoup.parse(content);
        String mainText = parsedDocument.getElementById("preview").text();
        ITuple tuple = new DataTuple(ZikaSchema.PromedMail_Schema, new IntegerField(idCounter), new TextField(mainText));
        idCounter++;
        return tuple;
    }


    public static void writePromedMailIndex() throws Exception {
        File sourceFileFolder = new File("./data-files/CrawlerResultPromed");
        ArrayList<String> fileContents = new ArrayList<>();
        for (File htmlFile : sourceFileFolder.listFiles()) {
            StringBuilder sb = new StringBuilder();
            Scanner scanner = new Scanner(htmlFile);
            while (scanner.hasNext()) {
                sb.append(scanner.nextLine());
            }
            scanner.close();
            fileContents.add(sb.toString());
        }

        ISourceOperator fileSource = new IterableSourceOperator(fileContents, (x -> parsePromedHTML(x)),
                ZikaSchema.PromedMail_Schema);

        IndexSink standardIndexSink = new IndexSink(standardIndexPath, ZikaSchema.PromedMail_Schema,
                new StandardAnalyzer());
        standardIndexSink.setInputOperator(fileSource);
        Plan standardIndexPlan = new Plan(standardIndexSink);

        IndexSink trigramIndexSink = new IndexSink(trigramIndexPath, ZikaSchema.PromedMail_Schema,
                DataConstants.getTrigramAnalyzer());
        trigramIndexSink.setInputOperator(fileSource);
        Plan trigramIndexPlan = new Plan(trigramIndexSink);

        Engine engine = Engine.getEngine();
        engine.evaluate(standardIndexPlan);
        engine.evaluate(trigramIndexPlan);
    }


    public static void extractPerson() throws Exception {
        String keyword = "zika";
        KeywordPredicate keywordPredicate = new KeywordPredicate(keyword, Arrays.asList(ZikaSchema.CONTENT_ATTR),
                new StandardAnalyzer(), DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);

        IOperator indexSource = new IndexBasedSourceOperator(keywordPredicate
                .generateDataReaderPredicate(new DataStore(standardIndexPath, ZikaSchema.PromedMail_Schema)));
        
        KeywordMatcher keywordMatcher = new KeywordMatcher(keywordPredicate);
        
        ProjectionPredicate projectionPredicate1 = new ProjectionPredicate(
                Arrays.asList(ZikaSchema.ID, ZikaSchema.CONTENT));
        ProjectionOperator projectionOperator1 = new ProjectionOperator(projectionPredicate1);

        String personRegex = "\\b(A|a|(an)|(An)) .{1,40} ((woman)|(man))\\b";
        RegexPredicate regexPredicate = new RegexPredicate(personRegex, Arrays.asList(ZikaSchema.CONTENT_ATTR),
                DataConstants.getTrigramAnalyzer());
        RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
        
        ProjectionPredicate projectionPredicate2 = new ProjectionPredicate(
                Arrays.asList(ZikaSchema.ID, SchemaConstants.SPAN_LIST));
        ProjectionOperator projectionOperator2 = new ProjectionOperator(projectionPredicate2);

        FileSink fileSink = new FileSink(
                new File("./data-files/results/PromedMail/person-result-"+PerfTestUtils.formatTime(System.currentTimeMillis())+".txt"));
        fileSink.setToStringFunction((tuple -> Utils.getTupleString(tuple)));

        keywordMatcher.setInputOperator(indexSource);
        projectionOperator1.setInputOperator(keywordMatcher);
        regexMatcher.setInputOperator(projectionOperator1);
        projectionOperator2.setInputOperator(regexMatcher);
        fileSink.setInputOperator(projectionOperator2);
        Plan extractPersonPlan = new Plan(fileSink);
        Engine.getEngine().evaluate(extractPersonPlan);

    }
    
    public static void extractLocation() throws Exception {
        String keyword = "zika";
        KeywordPredicate keywordPredicate = new KeywordPredicate(keyword, Arrays.asList(ZikaSchema.CONTENT_ATTR),
                new StandardAnalyzer(), DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);

        IOperator indexSource = new IndexBasedSourceOperator(keywordPredicate
                .generateDataReaderPredicate(new DataStore(standardIndexPath, ZikaSchema.PromedMail_Schema)));
        
        KeywordMatcher keywordMatcher = new KeywordMatcher(keywordPredicate);
        
        ProjectionPredicate projectionPredicate1 = new ProjectionPredicate(
                Arrays.asList(ZikaSchema.ID, ZikaSchema.CONTENT));
        ProjectionOperator projectionOperator1 = new ProjectionOperator(projectionPredicate1);

        NlpPredicate nlpPredicate = new NlpPredicate(NlpPredicate.NlpTokenType.Location, Arrays.asList(ZikaSchema.CONTENT_ATTR));
        NlpExtractor nlpExtractor = new NlpExtractor(nlpPredicate);
        
        ProjectionPredicate projectionPredicate2 = new ProjectionPredicate(
                Arrays.asList(ZikaSchema.ID, ZikaSchema.CONTENT, SchemaConstants.SPAN_LIST));
        ProjectionOperator projectionOperator2 = new ProjectionOperator(projectionPredicate2);

        FileSink fileSink = new FileSink( 
                new File("./data-files/results/PromedMail/location-result-"+PerfTestUtils.formatTime(System.currentTimeMillis())+".txt"));
        fileSink.setToStringFunction((tuple -> Utils.getTupleString(tuple)));
        
        keywordMatcher.setInputOperator(indexSource);
        projectionOperator1.setInputOperator(keywordMatcher);
        nlpExtractor.setInputOperator(projectionOperator1);
        projectionOperator2.setInputOperator(nlpExtractor);
        fileSink.setInputOperator(projectionOperator2);
        Plan extractPersonPlan = new Plan(fileSink);
        Engine.getEngine().evaluate(extractPersonPlan);
        
    }
    
    public static void extractPersonLocationDate() throws Exception {
        String keywordZika = "zika";
        KeywordPredicate keywordPredicateZika = new KeywordPredicate(keywordZika, Arrays.asList(ZikaSchema.CONTENT_ATTR),
                new StandardAnalyzer(), DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);

        IOperator indexSource = new IndexBasedSourceOperator(keywordPredicateZika
                .generateDataReaderPredicate(new DataStore(standardIndexPath, ZikaSchema.PromedMail_Schema)));
        
        KeywordMatcher keywordMatcherZika = new KeywordMatcher(keywordPredicateZika);
        
        ProjectionPredicate projectionPredicateIdAndContent = new ProjectionPredicate(
                Arrays.asList(ZikaSchema.ID, ZikaSchema.CONTENT));
        
        ProjectionOperator projectionOperatorIdAndContent1 = new ProjectionOperator(projectionPredicateIdAndContent);
        ProjectionOperator projectionOperatorIdAndContent2 = new ProjectionOperator(projectionPredicateIdAndContent);
        ProjectionOperator projectionOperatorIdAndContent3 = new ProjectionOperator(projectionPredicateIdAndContent);


        String regexPerson = "\\b(A|a|(an)|(An)) .{1,40} ((woman)|(man))\\b";
        RegexPredicate regexPredicatePerson = new RegexPredicate(regexPerson, Arrays.asList(ZikaSchema.CONTENT_ATTR),
                DataConstants.getTrigramAnalyzer());
        RegexMatcher regexMatcherPerson = new RegexMatcher(regexPredicatePerson);
        
        NlpPredicate nlpPredicateLocation = new NlpPredicate(NlpPredicate.NlpTokenType.Location, Arrays.asList(ZikaSchema.CONTENT_ATTR));
        NlpExtractor nlpExtractorLocation = new NlpExtractor(nlpPredicateLocation);
        
        // regexDate are found on regexr.com
        String regexDate = 
                "("+
                    "("+
                    "((0?[1-9])|(1[0-2]))"+"(\\s|-|.|\\/)"+
                    "((0?[1-9])|([12][0-9])|(3[01]))"+"(\\s|-|.|\\/)"+
                    "([0-9]{4}|[0-9]{2})"+
                    ")"+
                    // MM/DD/YYYY, MM-DD-YYYY, MM DD YYYY, MM.DD.YYYY
                "|"+
                    "("+
                    "((0?[1-9])|([12][0-9])|(3[01]))"+
                    " "+
                    "((jan(uary)?)|(feb(ruary)?)|(mar(ch)?)|(apr(il)?)|(may)|(june?)|(july?)|(aug(ust)?)|(sep(tember)?)|(oct(ober)?)|(nov(ember)?)|(dec(ember)?))"+
                    " "+
                    "([0-9]{4}|[0-9]{2})"+
                    ")"+
                    // DD Month YYYY
                ")";
        RegexPredicate regexPredicateDate = new RegexPredicate(regexDate, Arrays.asList(ZikaSchema.CONTENT_ATTR),
                DataConstants.getTrigramAnalyzer());
        RegexMatcher regexMatcherDate = new RegexMatcher(regexPredicateDate);
        
        IJoinPredicate joinPredicatePersonLocation = new JoinDistancePredicate(ZikaSchema.ID_ATTR, ZikaSchema.CONTENT_ATTR, 100);
        Join joinPersonLocation = new Join(joinPredicatePersonLocation);
        
        IJoinPredicate joinPredicatePersonLocationDate = new JoinDistancePredicate(ZikaSchema.ID_ATTR, ZikaSchema.CONTENT_ATTR, 100);
        Join joinPersonLocationDate = new Join(joinPredicatePersonLocationDate);
        
        ProjectionPredicate projectionPredicateIdAndSpan = new ProjectionPredicate(
                Arrays.asList(ZikaSchema.ID, SchemaConstants.SPAN_LIST));
        ProjectionOperator projectionOperatorIdAndSpan = new ProjectionOperator(projectionPredicateIdAndSpan);

        FileSink fileSink = new FileSink( 
                new File("./data-files/results/PromedMail/date-person-location-result-"+PerfTestUtils.formatTime(System.currentTimeMillis())+".txt"));
        fileSink.setToStringFunction((tuple -> Utils.getTupleString(tuple)));
        
        
        keywordMatcherZika.setInputOperator(indexSource);
        projectionOperatorIdAndContent1.setInputOperator(keywordMatcherZika);
        
        regexMatcherPerson.setInputOperator(projectionOperatorIdAndContent1);
        
        projectionOperatorIdAndContent2.setInputOperator(regexMatcherPerson);
        nlpExtractorLocation.setInputOperator(projectionOperatorIdAndContent2);
        
        joinPersonLocation.setInnerInputOperator(regexMatcherPerson);
        joinPersonLocation.setOuterInputOperator(nlpExtractorLocation);
        
        projectionOperatorIdAndContent3.setInputOperator(joinPersonLocation);
        regexMatcherDate.setInputOperator(projectionOperatorIdAndContent3); 
        
        joinPersonLocationDate.setInnerInputOperator(regexMatcherDate);
        joinPersonLocationDate.setOuterInputOperator(joinPersonLocation);
              
        projectionOperatorIdAndSpan.setInputOperator(joinPersonLocationDate);
        fileSink.setInputOperator(projectionOperatorIdAndSpan);
        
        Plan extractPersonPlan = new Plan(fileSink);
        Engine.getEngine().evaluate(extractPersonPlan);
    }
    
    
    public static void extractPersonLocationDate2() throws Exception {
        String keywordZika = "zika";
        KeywordPredicate keywordPredicateZika = new KeywordPredicate(keywordZika, Arrays.asList(ZikaSchema.CONTENT_ATTR),
                new StandardAnalyzer(), DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);

        IOperator indexSource = new IndexBasedSourceOperator(keywordPredicateZika
                .generateDataReaderPredicate(new DataStore(standardIndexPath, ZikaSchema.PromedMail_Schema)));
        
        KeywordMatcher keywordMatcherZika = new KeywordMatcher(keywordPredicateZika);
        
        ProjectionPredicate projectionPredicateIdAndContent = new ProjectionPredicate(
                Arrays.asList(ZikaSchema.ID, ZikaSchema.CONTENT));
        
        ProjectionOperator projectionOperatorIdAndContent1 = new ProjectionOperator(projectionPredicateIdAndContent);
        ProjectionOperator projectionOperatorIdAndContent2 = new ProjectionOperator(projectionPredicateIdAndContent);
        ProjectionOperator projectionOperatorIdAndContent3 = new ProjectionOperator(projectionPredicateIdAndContent);
        
        OneToNBroadcastConnector keywordToProjectionConnector = new OneToNBroadcastConnector(3);

        String regexPerson = "\\b(woman)|(man)|(patient)\\b";
        RegexPredicate regexPredicatePerson = new RegexPredicate(regexPerson, Arrays.asList(ZikaSchema.CONTENT_ATTR),
                DataConstants.getTrigramAnalyzer());
        RegexMatcher regexMatcherPerson = new RegexMatcher(regexPredicatePerson);
        
        NlpPredicate nlpPredicateLocation = new NlpPredicate(NlpPredicate.NlpTokenType.Location, Arrays.asList(ZikaSchema.CONTENT_ATTR));
        NlpExtractor nlpExtractorLocation = new NlpExtractor(nlpPredicateLocation);
        
        // regexDate are found on regexr.com
        String regexDate = 
                "("+
                    "("+
                    "((0?[1-9])|(1[0-2]))"+"(\\s|-|.|\\/)"+
                    "((0?[1-9])|([12][0-9])|(3[01]))"+"(\\s|-|.|\\/)"+
                    "([0-9]{4}|[0-9]{2})"+
                    ")"+
                    // MM/DD/YYYY, MM-DD-YYYY, MM DD YYYY, MM.DD.YYYY
                "|"+
                    "("+
                    "((0?[1-9])|([12][0-9])|(3[01]))"+
                    " "+
                    "((jan(uary)?)|(feb(ruary)?)|(mar(ch)?)|(apr(il)?)|(may)|(june?)|(july?)|(aug(ust)?)|(sep(tember)?)|(oct(ober)?)|(nov(ember)?)|(dec(ember)?))"+
                    " "+
                    "([0-9]{4}|[0-9]{2})"+
                    ")"+
                    // DD Month YYYY
                ")";
        RegexPredicate regexPredicateDate = new RegexPredicate(regexDate, Arrays.asList(ZikaSchema.CONTENT_ATTR),
                DataConstants.getTrigramAnalyzer());
        RegexMatcher regexMatcherDate = new RegexMatcher(regexPredicateDate);
    
        IJoinPredicate joinPredicatePersonLocation = new JoinDistancePredicate(ZikaSchema.ID_ATTR, ZikaSchema.CONTENT_ATTR, 100);
        Join joinPersonLocation = new Join(joinPredicatePersonLocation);
        
        IJoinPredicate joinPredicatePersonLocationDate = new JoinDistancePredicate(ZikaSchema.ID_ATTR, ZikaSchema.CONTENT_ATTR, 100);
        Join joinPersonLocationDate = new Join(joinPredicatePersonLocationDate);
        
        ProjectionPredicate projectionPredicateIdAndSpan = new ProjectionPredicate(
                Arrays.asList(ZikaSchema.ID, SchemaConstants.SPAN_LIST));
        ProjectionOperator projectionOperatorIdAndSpan = new ProjectionOperator(projectionPredicateIdAndSpan);

        FileSink fileSink = new FileSink( 
                new File("./data-files/results/PromedMail/date-person-location-result-"+PerfTestUtils.formatTime(System.currentTimeMillis())+".txt"));
        fileSink.setToStringFunction((tuple -> Utils.getTupleString(tuple)));
        
        
        keywordMatcherZika.setInputOperator(indexSource);
        
        keywordToProjectionConnector.setInputOperator(keywordMatcherZika);
        
        projectionOperatorIdAndContent1.setInputOperator(keywordToProjectionConnector.getOutputOperator(0));
        projectionOperatorIdAndContent2.setInputOperator(keywordToProjectionConnector.getOutputOperator(1));
        projectionOperatorIdAndContent3.setInputOperator(keywordToProjectionConnector.getOutputOperator(2));

                
        regexMatcherPerson.setInputOperator(projectionOperatorIdAndContent1);
        nlpExtractorLocation.setInputOperator(projectionOperatorIdAndContent2);
        regexMatcherDate.setInputOperator(projectionOperatorIdAndContent3);
        
        joinPersonLocation.setInnerInputOperator(regexMatcherPerson);
        joinPersonLocation.setOuterInputOperator(nlpExtractorLocation);
               
        joinPersonLocationDate.setInnerInputOperator(regexMatcherDate);
        joinPersonLocationDate.setOuterInputOperator(joinPersonLocation);
              
        projectionOperatorIdAndSpan.setInputOperator(joinPersonLocationDate);
        fileSink.setInputOperator(projectionOperatorIdAndSpan);
        
        Plan extractPersonPlan = new Plan(fileSink);
        Engine.getEngine().evaluate(extractPersonPlan);
    }
    


}
