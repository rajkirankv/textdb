package edu.uci.ics.textdb.perftest.zika_playground;

import java.util.Arrays;

import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.engine.Engine;
import edu.uci.ics.textdb.storage.DataStore;

public class ZikaAttempt {
	
	private static String filePath = "./file_path.txt";
	private static String indexPath = "./index/testindex";
	
	public static void main(String[] args) throws Exception {
		writeIndex();
	}
	
	public static void writeIndex() throws Exception {
		Engine engine = Engine.getEngine();
		engine.evaluate(ZikaSchema.writeIndexPlan(filePath, indexPath));
	}
	
	public static void extractPerson() throws Exception {
		String personRegex = "(A|a|(an)|(An)) .{1,40} ((woman)|(man))";
		RegexPredicate regexPredicate = new RegexPredicate(personRegex, new DataStore(indexPath, ZikaSchema.PromedMail_Schema), Arrays.asList(ZikaSchema.CONTENT_ATTR), DataConstants.getTrigramAnalyzer());
	}
	

}
