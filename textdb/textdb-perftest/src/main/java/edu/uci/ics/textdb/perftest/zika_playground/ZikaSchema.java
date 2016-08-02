package edu.uci.ics.textdb.perftest.zika_playground;

import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.plan.Plan;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.dataflow.sink.IndexSink;
import edu.uci.ics.textdb.dataflow.source.FileSourceOperator;

public class ZikaSchema {
	
	public static String CONTENT = "content";
	
	public static Attribute CONTENT_ATTR = new Attribute(CONTENT, FieldType.TEXT);
	
	public static Schema PromedMail_Schema = new Schema(new Attribute[]{CONTENT_ATTR});
	
	public static Plan writeIndexPlan(String filePath, String indexPath) {
		FileSourceOperator fileSource = new FileSourceOperator(filePath, (x -> new DataTuple(PromedMail_Schema, new IField[]{new TextField(x)})));
		IndexSink sink = new IndexSink(fileSource, indexPath, PromedMail_Schema, new StandardAnalyzer());
		Plan plan = new Plan(sink);
		return plan;
	}

}
