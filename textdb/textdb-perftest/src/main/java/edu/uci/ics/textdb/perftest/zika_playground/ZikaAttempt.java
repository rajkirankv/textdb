package edu.uci.ics.textdb.perftest.zika_playground;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;
import edu.uci.ics.textdb.api.plan.Plan;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexMatcher;
import edu.uci.ics.textdb.dataflow.sink.FileSink;
import edu.uci.ics.textdb.dataflow.sink.IndexSink;
import edu.uci.ics.textdb.dataflow.source.IterableSourceOperator;
import edu.uci.ics.textdb.engine.Engine;
import edu.uci.ics.textdb.storage.DataStore;

public class ZikaAttempt {
	
	private static String standardIndexPath = "./index/testindex/promed/standard/";
	private static String trigramIndexPath = "./index/testindex/promed/trigram/";
	
	private static String filteredStandardIndexPath = "./index/testindex/promed_filtered/standard/";
	private static String filteredTrigramIndexPath = "./index/testindex/promed_filtered/trigram/";

	
	public static void main(String[] args) throws Exception {
//		writeIndex();
		extractPerson();
	}
	
	public static ITuple parsePromedHTML(String content) throws Exception {
		Document parsedDocument = Jsoup.parse(content);
		String mainText = parsedDocument.getElementById("preview").text();	
		ITuple tuple = new DataTuple(ZikaSchema.PromedMail_Schema, new TextField(mainText));
		return tuple;
	}
	
	public static void writeIndex() throws Exception {
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
		
		ISourceOperator fileSource = new IterableSourceOperator(fileContents, (x -> parsePromedHTML(x)));
		
		IndexSink standardIndexSink = new IndexSink(fileSource, standardIndexPath, ZikaSchema.PromedMail_Schema, new StandardAnalyzer());
		Plan standardIndexPlan = new Plan(standardIndexSink);
		
		IndexSink trigramIndexSink = new IndexSink(fileSource, trigramIndexPath, ZikaSchema.PromedMail_Schema, DataConstants.getTrigramAnalyzer());
		Plan trigramIndexPlan = new Plan(trigramIndexSink);
		
		Engine engine = Engine.getEngine();
		engine.evaluate(standardIndexPlan);
		engine.evaluate(trigramIndexPlan);
	}
	
	public static void filterZika() throws Exception {
		String keyword = "zika";
		KeywordPredicate keywordPredicate = new KeywordPredicate(keyword,
				new DataStore(standardIndexPath, ZikaSchema.PromedMail_Schema),
				Arrays.asList(ZikaSchema.CONTENT_ATTR),
				new StandardAnalyzer(),
				DataConstants.KeywordMatchingType.CONJUNCTION_INDEXBASED);
		KeywordMatcher keywordMatcher = new KeywordMatcher(keywordPredicate);
		
		IndexSink indexSinkStandard = new IndexSink(keywordMatcher, filteredStandardIndexPath, ZikaSchema.PromedMail_Schema, new StandardAnalyzer());
		Plan filterZikaPlanStandard = new Plan(indexSinkStandard);
		IndexSink indexSinkTrigram = new IndexSink(keywordMatcher, filteredTrigramIndexPath, ZikaSchema.PromedMail_Schema, new StandardAnalyzer());
		Plan filterZikaPlanTrigram = new Plan(indexSinkTrigram);
		
		Engine.getEngine().evaluate(filterZikaPlanStandard);
		Engine.getEngine().evaluate(filterZikaPlanTrigram);
	}
	
	
	public static void extractPerson() throws Exception {
		String personRegex = "(A|a|(an)|(An)) .{1,40} ((woman)|(man))";
		RegexPredicate regexPredicate = new RegexPredicate(personRegex, 
				new DataStore(trigramIndexPath, ZikaSchema.PromedMail_Schema), 
				Arrays.asList(ZikaSchema.CONTENT_ATTR), 
				DataConstants.getTrigramAnalyzer());
		RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
		
		FileSink fileSink = new FileSink(regexMatcher, 
				new File("./data-files/results/PromedMail/result_8_2_3pm.txt"), 
				(tuple -> Utils.getTupleString(tuple)));
		
		Plan extractPersonPlan = new Plan(fileSink);
		
		Engine engine = Engine.getEngine();
		engine.evaluate(extractPersonPlan);
	}
	

}
