package edu.uci.ics.textdb.textql.statements.predicates;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.plangen.operatorbuilder.KeywordMatcherBuilder;
import edu.uci.ics.textdb.textql.statements.StatementTestUtils;
import edu.uci.ics.textdb.web.request.beans.KeywordMatcherBean;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;

/**
 * This class contains test cases for the KeywordExtractPredicate class.
 * The constructor, getters, setters and the getOperatorBean methods are
 * tested.
 * 
 * @author Flavio Bayer
 *
 */
public class KeywordExtractPredicateTest {

    /**
     * Test the class constructor, getters and the setter methods.
     * Call the constructor of the KeywordExtractPredicate, test 
     * if the returned value by the getter is the same as used in 
     * the constructor and then test if the value is changed
     * when the setter method is invoked.
     */
    @Test
    public void testConstructorsGettersSetters(){
        List<String> matchingFields;
        String keywords;
        String matchingType;

        matchingFields = null;
        keywords = null;
        matchingType = null;
        assertConstructorGettersSetters(matchingFields, keywords, matchingType);
        
        matchingFields = Arrays.asList();
        keywords = "xxx";
        matchingType = KeywordMatchingType.PHRASE_INDEXBASED.toString();
        assertConstructorGettersSetters(matchingFields, keywords, matchingType);
        
        matchingFields = Arrays.asList("a", "b");
        keywords = "new york";
        matchingType = KeywordMatchingType.CONJUNCTION_INDEXBASED.toString();
        assertConstructorGettersSetters(matchingFields, keywords, matchingType);
        
        matchingFields = Arrays.asList("field1", "field0", "field2");
        keywords = "university of california irvine";
        matchingType = KeywordMatchingType.SUBSTRING_SCANBASED.toString();
        assertConstructorGettersSetters(matchingFields, keywords, matchingType);
    }

    /**
     * Assert the correctness of the Constructor, getter and setter methods.
     * Test the constructor with the given parameters, no parameters, the setters
     * with the given values, the setters with null values. The getter methods are
     * used to get the value back from the object being tested.
     * @param matchingFields The matchingFields value of the KeywordExtractPredicate.
     * @param keywords The keywords value of the KeywordExtractPredicate.
     * @param matchingType The matchingType value of the KeywordExtractPredicate.
     */
    private void assertConstructorGettersSetters(List<String> matchingFields, String keywords, String matchingType){
        KeywordExtractPredicate keywordExtractPredicate;
        
        // Check the constructor with the arguments for initialization
        keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        Assert.assertEquals(keywordExtractPredicate.getMatchingFields(), matchingFields);
        Assert.assertEquals(keywordExtractPredicate.getKeywords(), keywords);
        Assert.assertEquals(keywordExtractPredicate.getMatchingType(), matchingType);
        
        // Check the constructor with no arguments
        keywordExtractPredicate = new KeywordExtractPredicate();
        Assert.assertEquals(keywordExtractPredicate.getMatchingFields(), null);
        Assert.assertEquals(keywordExtractPredicate.getKeywords(), null);
        Assert.assertEquals(keywordExtractPredicate.getMatchingType(), null);
        
        // Check all the setters with the given values
        keywordExtractPredicate.setMatchingFields(matchingFields);
        Assert.assertEquals(keywordExtractPredicate.getMatchingFields(), matchingFields);
        keywordExtractPredicate.setKeywords(keywords);
        Assert.assertEquals(keywordExtractPredicate.getKeywords(), keywords);
        keywordExtractPredicate.setMatchingType(matchingType);
        Assert.assertEquals(keywordExtractPredicate.getMatchingType(), matchingType);
        
        // Check all the setters with null values
        keywordExtractPredicate.setMatchingFields(null);
        Assert.assertEquals(keywordExtractPredicate.getMatchingFields(), null);
        keywordExtractPredicate.setKeywords(null);
        Assert.assertEquals(keywordExtractPredicate.getKeywords(), null);
        keywordExtractPredicate.setMatchingType(null);
        Assert.assertEquals(keywordExtractPredicate.getMatchingType(), null);
    }

    /**
     * Test the getOperatorBean method.
     * Build a KeywordExtractPredicate, invoke the getOperatorBean and
     * check whether a KeywordMatcherBean with the right attributes is returned.
     * An empty list is used as the list of fields to perform the match.
     */
    @Test
    public void testGetOperatorBean00() {
        List<String> matchingFields = Collections.emptyList();
        String keywords = "keyword";
        String matchingType = KeywordMatchingType.CONJUNCTION_INDEXBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        OperatorBean computedProjectionBean = keywordExtractPredicate.getOperatorBean("xxx");
        String matchingFieldsAsString = String.join(",", matchingFields);
        OperatorBean expectedProjectionBean = new KeywordMatcherBean("xxx", "KeywordMatcher", matchingFieldsAsString,
                            null, null, keywords, matchingType);
        
        Assert.assertEquals(expectedProjectionBean, computedProjectionBean);
    }
    
    /**
     * Test the getOperatorBean method.
     * Build a KeywordExtractPredicate, invoke the getOperatorBean and
     * check whether a KeywordMatcherBean with the right attributes is returned.
     * A list with one field is used as the list of fields to perform the match.
     */
    @Test
    public void testGetOperatorBean01() {
        List<String> matchingFields = Arrays.asList("fieldOne");
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.PHRASE_INDEXBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        OperatorBean computedProjectionBean = keywordExtractPredicate.getOperatorBean("operator");
        String matchingFieldsAsString = String.join(",", matchingFields);
        OperatorBean expectedProjectionBean = new KeywordMatcherBean("operator", "KeywordMatcher", matchingFieldsAsString,
                            null, null, keywords, matchingType);
        
        Assert.assertEquals(expectedProjectionBean, computedProjectionBean);
    }
    
    /**
     * Test the getOperatorBean method.
     * Build a KeywordExtractPredicate, invoke the getOperatorBean and
     * check whether a KeywordMatcherBean with the right attributes is returned.
     * A list with some fields is used as the list of fields to perform the match.
     */
    @Test
    public void testGetOperatorBean02() {
        List<String> matchingFields = Arrays.asList("field0", "field1");
        String keywords = "xxx";
        String matchingType = KeywordMatchingType.SUBSTRING_SCANBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        OperatorBean computedProjectionBean = keywordExtractPredicate.getOperatorBean("keywordExtract00");
        String matchingFieldsAsString = String.join(",", matchingFields);
        OperatorBean expectedProjectionBean = new KeywordMatcherBean("keywordExtract00", "KeywordMatcher",
                            matchingFieldsAsString, null, null, keywords, matchingType);
        
        Assert.assertEquals(expectedProjectionBean, computedProjectionBean);
    }

    /**
     * Test the generateOutputSchema method.
     * This test use an empty Schema as input and no fields for extraction
     * to be performed.
     * The expected output Schema is a schema with the PAYLAOAD and
     * SPAN_LIST attribute.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema00() throws TextDBException {
        List<String> matchingFields = Arrays.asList();
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.CONJUNCTION_INDEXBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        Schema inputSchema = new Schema();
                
        Schema computedOutputSchema = keywordExtractPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = new Schema(
                SchemaConstants.PAYLOAD_ATTRIBUTE,
                SchemaConstants.SPAN_LIST_ATTRIBUTE
            );
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }

    /**
     * Test the generateOutputSchema method.
     * This test use a Schema with all the values of FiledType as input
     * and no field to be matched.
     * The expected output Schema is the input schema with the added 
     * PAYLAOAD and SPAN_LIST attributes.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema01() throws TextDBException {
        List<String> matchingFields = Arrays.asList();
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.PHRASE_INDEXBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
                
        Schema computedOutputSchema = keywordExtractPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = new Schema(Stream.concat(
                StatementTestUtils.ALL_FIELD_TYPES_SCHEMA.getAttributes().stream(),
                Stream.of(SchemaConstants.PAYLOAD_ATTRIBUTE, SchemaConstants.SPAN_LIST_ATTRIBUTE)
            ).toArray(Attribute[]::new));
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }

    /**
     * Test the generateOutputSchema method.
     * This test use a Schema with attributes in which FiledType is 
     * either String or Text. All the fields in the input Schema are
     * to be matched.
     * The expected output Schema is the input schema with the added
     * PAYLAOAD and SPAN_LIST attributes.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema02() throws TextDBException {
        List<String> matchingFields = Arrays.asList(
                StatementTestUtils.STRING_ATTRIBUTE.getFieldName(),
                StatementTestUtils.TEXT_ATTRIBUTE.getFieldName()
            );
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.SUBSTRING_SCANBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        Schema inputSchema = new Schema(
                StatementTestUtils.STRING_ATTRIBUTE,
                StatementTestUtils.TEXT_ATTRIBUTE
            );
                
        Schema computedOutputSchema = keywordExtractPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = new Schema(
                StatementTestUtils.STRING_ATTRIBUTE,
                StatementTestUtils.TEXT_ATTRIBUTE,
                SchemaConstants.PAYLOAD_ATTRIBUTE,
                SchemaConstants.SPAN_LIST_ATTRIBUTE
            );
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }
    
    /**
     * Test the generateOutputSchema method.
     * This test use a Schema with all the values of FiledType as input
     * and attributes with type String and Text to be extracted.
     * The expected output Schema is the input schema with the added 
     * PAYLAOAD and SPAN_LIST attributes.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema03() throws TextDBException {
        List<String> matchingFields = Arrays.asList(
                StatementTestUtils.STRING_ATTRIBUTE.getFieldName(),
                StatementTestUtils.TEXT_ATTRIBUTE.getFieldName()
            );
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.SUBSTRING_SCANBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
                
        Schema computedOutputSchema = keywordExtractPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = new Schema(Stream.concat(
                StatementTestUtils.ALL_FIELD_TYPES_SCHEMA.getAttributes().stream(),
                Stream.of(SchemaConstants.PAYLOAD_ATTRIBUTE, SchemaConstants.SPAN_LIST_ATTRIBUTE)
            ).toArray(Attribute[]::new));
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }

    /**
     * Test the generateOutputSchema method.
     * This test use a Schema with all the values of FiledType and the 
     * PAYLAOAD attribute as input. One field with type String is
     * in the list of fields to perform the extraction.
     * The expected output Schema is the input schema with the added 
     * SPAN_LIST attribute.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema04() throws TextDBException {
        List<String> matchingFields = Arrays.asList(
                StatementTestUtils.STRING_ATTRIBUTE.getFieldName()
            );
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.SUBSTRING_SCANBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        Schema inputSchema = new Schema(Stream.concat(
                StatementTestUtils.ALL_FIELD_TYPES_SCHEMA.getAttributes().stream(),
                Stream.of(SchemaConstants.PAYLOAD_ATTRIBUTE)
            ).toArray(Attribute[]::new));
                
        Schema computedOutputSchema = keywordExtractPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = new Schema(Stream.concat(
                StatementTestUtils.ALL_FIELD_TYPES_SCHEMA.getAttributes().stream(),
                Stream.of(SchemaConstants.PAYLOAD_ATTRIBUTE, SchemaConstants.SPAN_LIST_ATTRIBUTE)
            ).toArray(Attribute[]::new));
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }
    
    /**
     * Test the generateOutputSchema method.
     * This test use a Schema with all the values of FiledType and the 
     * SPAN_LIST attribute as input. One field with type Text is
     * in the list of fields to perform the extraction.
     * The expected output Schema is the input schema with the added 
     * PAYLAOAD attribute.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema05() throws TextDBException {
        List<String> matchingFields = Arrays.asList(
                StatementTestUtils.TEXT_ATTRIBUTE.getFieldName()
            );
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.SUBSTRING_SCANBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        Schema inputSchema = new Schema(Stream.concat(
                StatementTestUtils.ALL_FIELD_TYPES_SCHEMA.getAttributes().stream(),
                Stream.of(SchemaConstants.SPAN_LIST_ATTRIBUTE)
            ).toArray(Attribute[]::new));
                
        Schema computedOutputSchema = keywordExtractPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = new Schema(Stream.concat(
                StatementTestUtils.ALL_FIELD_TYPES_SCHEMA.getAttributes().stream(),
                Stream.of(SchemaConstants.SPAN_LIST_ATTRIBUTE, SchemaConstants.PAYLOAD_ATTRIBUTE)
            ).toArray(Attribute[]::new));
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }
    
    /**
     * Test the generateOutputSchema method.
     * This test use a Schema with all the values of FiledType, the 
     * SPAN_LIST and the PAYLOAD attributes as input. One field with
     * type String is in the list of fields to be matched.
     * The expected output Schema is a schema equal to the input schema.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema06() throws TextDBException {
        List<String> matchingFields = Arrays.asList(
                StatementTestUtils.STRING_ATTRIBUTE.getFieldName()
            );
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.SUBSTRING_SCANBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        Schema inputSchema = new Schema(Stream.concat(
                StatementTestUtils.ALL_FIELD_TYPES_SCHEMA.getAttributes().stream(),
                Stream.of(SchemaConstants.SPAN_LIST_ATTRIBUTE, SchemaConstants.PAYLOAD_ATTRIBUTE)
            ).toArray(Attribute[]::new));
                
        Schema computedOutputSchema = keywordExtractPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = inputSchema;
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }
    
    /**
     * Test the generateOutputSchema method.
     * This test use a Schema with all the values of FiledType. One
     * missing field is in the list of fields to perform the matching.
     * The expected result is a TextDBException being thrown, since field
     * 'fieldInvalid' does not exist in the input schema.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test(expected = TextDBException.class)
    public void testGenerateOutputSchema07() throws TextDBException {
        List<String> matchingFields = Arrays.asList(
                StatementTestUtils.STRING_ATTRIBUTE.getFieldName(),
                "fieldInvalid",
                StatementTestUtils.TEXT_ATTRIBUTE.getFieldName()
            );
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.SUBSTRING_SCANBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
                
        keywordExtractPredicate.generateOutputSchema(inputSchema);
    }
    
    /**
     * Test the generateOutputSchema method.
     * This test use a Schema with all the values of FiledType. One
     * field with incompatible is in the list of fields to perform
     * the matching.
     * The expected result is a TextDBException being thrown, since 
     * fieldType Integer is not compatible with the matching type of
     * this predicate.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test(expected = TextDBException.class)
    public void testGenerateOutputSchema08() throws TextDBException {
        List<String> matchingFields = Arrays.asList(
                StatementTestUtils.STRING_ATTRIBUTE.getFieldName(),
                StatementTestUtils.INTEGER_ATTRIBUTE.getFieldName(),
                StatementTestUtils.TEXT_ATTRIBUTE.getFieldName()
            );
        String keywords = "keyword(s)";
        String matchingType = KeywordMatchingType.SUBSTRING_SCANBASED.toString();
        KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
                
        keywordExtractPredicate.generateOutputSchema(inputSchema);
    }
    
    /**
     * Test the generateOutputSchema method.
     * This test check if valid values for matchingType with different
     * cases can successfully generate an outputSchema.
     * This test use a Schema with all the values of FiledType and a
     * list with no fields to be matched.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema09() throws TextDBException {
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
        // Create a list with valid matchingType with the original case, lower case and upper case
        List<String> allValidMatchingTypes = KeywordMatcherBuilder.keywordMatchingTypeMap.keySet().stream()
                .flatMap( matchingType -> Stream.of(matchingType, matchingType.toLowerCase(), matchingType.toUpperCase()) )
                .collect( Collectors.toList() );
        
        for (String matchingType : allValidMatchingTypes) {
            // Build the predicate
            List<String> matchingFields = Collections.emptyList();
            String keywords = "keyword(s)";
            KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
            // Get the output schema from the predicate
            Schema computedOutputSchema = keywordExtractPredicate.generateOutputSchema(inputSchema);
            Schema expectedOutputSchema = new Schema(Stream.concat(
                    StatementTestUtils.ALL_FIELD_TYPES_SCHEMA.getAttributes().stream(),
                    Stream.of(SchemaConstants.PAYLOAD_ATTRIBUTE, SchemaConstants.SPAN_LIST_ATTRIBUTE)
                ).toArray(Attribute[]::new));
            
            Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
        }
    }
    
    /**
     * Test the generateOutputSchema method.
     * This test check if a invalid matchingType (different cases)
     * will cause an TextDBException to be thrown.
     * This test use a Schema with all the values of FiledType. A
     * list with no fields to be matched is used.
     */
    @Test
    public void testGenerateOutputSchema10() {
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
        // Create a list with invalid values for the matchingType attribute (perform some operations to change the string)
        Set<String> allValidMatchingTypes = KeywordMatcherBuilder.keywordMatchingTypeMap.keySet();
        List<String> invalidMatchingTypes = allValidMatchingTypes.stream()
                .map( (validMatchingType) -> validMatchingType.substring(1, validMatchingType.length()/2))
                .map( (invalidMatchingType) -> invalidMatchingType.substring(invalidMatchingType.length()/2)+invalidMatchingType )
                .flatMap( matchingType -> Stream.of(matchingType, matchingType.toLowerCase(), matchingType.toUpperCase()) )
                .filter( invalidMatchingType -> !allValidMatchingTypes.contains(invalidMatchingType))
                .collect(Collectors.toList());
        Assert.assertTrue(!invalidMatchingTypes.isEmpty());
        
        for (String matchingType : invalidMatchingTypes) {
            // Build the predicate
            List<String> matchingFields = Collections.emptyList();
            String keywords = "keyword(s)";
            KeywordExtractPredicate keywordExtractPredicate = new KeywordExtractPredicate(matchingFields, keywords, matchingType);
            // Get the output schema from the predicate
            try {
                keywordExtractPredicate.generateOutputSchema(inputSchema);
                Assert.fail("Expected generateOutputSchema method to throw a TextDBException");
            }catch(TextDBException e){
                
            }
        }
    }
    
}
