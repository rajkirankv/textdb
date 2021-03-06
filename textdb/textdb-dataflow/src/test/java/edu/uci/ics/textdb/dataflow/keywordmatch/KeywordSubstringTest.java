package edu.uci.ics.textdb.dataflow.keywordmatch;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.constants.TestConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.DateField;
import edu.uci.ics.textdb.common.field.DoubleField;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;
import edu.uci.ics.textdb.dataflow.utils.TestUtils;

/**
 * @author ZhenfengQi
 *
 */
public class KeywordSubstringTest {

    public static final String PEOPLE_TABLE = KeywordTestHelper.PEOPLE_TABLE;
    public static final String MEDLINE_TABLE = KeywordTestHelper.MEDLINE_TABLE;
    
    public static final KeywordMatchingType substring = KeywordMatchingType.SUBSTRING_SCANBASED;
    
    @BeforeClass
    public static void setUp() throws Exception {
        KeywordTestHelper.writeTestTables();
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        KeywordTestHelper.deleteTestTables();
    }


    /**
     * Verifies Substring Matcher where Query phrase does not exist in any
     * document.
     * 
     * @throws Exception
     */
    @Test
    public void testSubstringMatcher() throws Exception {
        // Prepare Query
        String query = "brad and angelina";
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.FIRST_NAME);
        attributeNames.add(TestConstants.LAST_NAME);
        attributeNames.add(TestConstants.DESCRIPTION);

        // Perform Query
        List<ITuple> results = KeywordTestHelper.getScanSourceResults(PEOPLE_TABLE, query, attributeNames, substring, Integer.MAX_VALUE, 0);

        // Perform Check
        Assert.assertEquals(0, results.size());
    }

    /**
     * Verifies List<ITuple> returned by Substring Matcher on query with
     * multiple spaces and stop words on a String Field
     * 
     * @throws Exception
     */
    @Test
    public void testSubstringForStringField() throws Exception {
        // Prepare Query
        String query = "short and lin";
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.FIRST_NAME);
        attributeNames.add(TestConstants.LAST_NAME);
        attributeNames.add(TestConstants.DESCRIPTION);

        // Prepare expected result list
        List<Span> list = new ArrayList<Span>();
        Span span1 = new Span("description", 15, 28, "short and lin", "Short and lin");
        list.add(span1);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<>(list) };

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        // Perform Query
        List<ITuple> resultList = KeywordTestHelper.getScanSourceResults(PEOPLE_TABLE, query, attributeNames, substring, Integer.MAX_VALUE, 0);

        // Perform Check
        boolean contains = TestUtils.equals(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }

    /**
     * Verifies: Verifies List<ITuple> returned multiple results by Substring
     * Matcher on query with spaces on both left side and right side.
     * 
     * @throws Exception
     */
    @Test
    public void testCombinedSpanInMultipleFieldsQuery() throws Exception {
        // Prepare Query
        String query = " lin ";
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.FIRST_NAME);
        attributeNames.add(TestConstants.LAST_NAME);
        attributeNames.add(TestConstants.DESCRIPTION);

        // Prepare expected result list
        List<Span> list = new ArrayList<>();
        Span span1 = new Span("description", 24, 29, " lin ", " lin ");

        list.add(span1);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<>(list) };

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        // Perform Query
        List<ITuple> resultList = KeywordTestHelper.getScanSourceResults(PEOPLE_TABLE, query, attributeNames, substring, Integer.MAX_VALUE, 0);

        // Perform Check
        boolean contains = TestUtils.equals(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }

    /**
     * Verifies: Verifies List<ITuple> returned multiple results by Substring
     * Matcher on query with spaces on both left side and right side.
     * 
     * @throws Exception
     */
    @Test
    public void testSubstringWithStopwordQuery() throws Exception {
        // Prepare Query
        String query = "is";
        ArrayList<String> attributeNames = new ArrayList<>();
        attributeNames.add(TestConstants.FIRST_NAME);
        attributeNames.add(TestConstants.LAST_NAME);
        attributeNames.add(TestConstants.DESCRIPTION);

        // Prepare expected result list
        List<Span> list = new ArrayList<>();
        Span span1 = new Span("description", 12, 14, "is", "is");
        Span span2 = new Span("description", 37, 39, "is", "is");

        list.add(span1);
        list.add(span2);

        Attribute[] schemaAttributes = new Attribute[TestConstants.ATTRIBUTES_PEOPLE.length + 1];
        for (int count = 0; count < schemaAttributes.length - 1; count++) {
            schemaAttributes[count] = TestConstants.ATTRIBUTES_PEOPLE[count];
        }
        schemaAttributes[schemaAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;

        IField[] fields1 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                new TextField("Lin Clooney is Short and lin clooney is Angry"), new ListField<>(list) };

        ITuple tuple1 = new DataTuple(new Schema(schemaAttributes), fields1);
        List<ITuple> expectedResultList = new ArrayList<>();
        expectedResultList.add(tuple1);

        // Perform Query
        List<ITuple> resultList = KeywordTestHelper.getScanSourceResults(PEOPLE_TABLE, query, attributeNames, substring, Integer.MAX_VALUE, 0);

        // Perform Check
        boolean contains = TestUtils.equals(expectedResultList, resultList);
        Assert.assertTrue(contains);
    }

}
