package edu.uci.ics.textdb.perftest.chinesenews;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;

public class ChineseNews {
    
    public static final String URL = "url";
    public static final String TITLE = "title";
    public static final String CONTENT = "content";
    
    public static final Attribute URL_ATTR = new Attribute(URL, FieldType.STRING);
    public static final Attribute TITLE_ATTR = new Attribute(TITLE, FieldType.TEXT);
    public static final Attribute CONTENT_ATTR = new Attribute(CONTENT, FieldType.TEXT);
    
    public static final Schema CHINESE_NEWS_SCHEMA = new Schema(
            URL_ATTR, TITLE_ATTR, CONTENT_ATTR);
    

}
