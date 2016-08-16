package edu.uci.ics.textdb.perftest.zika_playground;


import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.Schema;

public class ZikaSchema {
	
    public static String ID = "id";
	public static String CONTENT = "content";
	
	public static Attribute ID_ATTR = new Attribute(ID, FieldType.INTEGER);
	public static Attribute CONTENT_ATTR = new Attribute(CONTENT, FieldType.TEXT);
	
	public static Schema PromedMail_Schema = new Schema(new Attribute[]{ID_ATTR, CONTENT_ATTR});
	


}
