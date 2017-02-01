package edu.uci.ics.textdb.textql.statements.predicates;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.dataflow.projection.ProjectionOperator;
import edu.uci.ics.textdb.plangen.PlanGenUtils;
import edu.uci.ics.textdb.textql.statements.StatementTestUtils;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;
import edu.uci.ics.textdb.web.request.beans.ProjectionBean;

/**
 * This class contains test cases for the SelectSomeFieldsPredicate class.
 * The constructor, getters, setters and the generateOperatorBean methods are
 * tested.
 * 
 * @author Flavio Bayer
 *
 */
public class SelectSomeFieldsPredicateTest {
    
    /**
     * Test the class constructor, getter and the setter methods.
     * Call the constructor of the SelectSomeFieldsPredicate, test 
     * if the returned value by the getter is the same as used in 
     * the constructor and then test if the value is changed
     * when the setter method is invoked.
     */
    @Test
    public void testConstructorsGettersSetters(){
        List<String> projectedFields;

        projectedFields = Collections.emptyList();
        assertConstructorGettersSetters(projectedFields);
        
        projectedFields = Arrays.asList("a","b","c","d");
        assertConstructorGettersSetters(projectedFields);
        
        projectedFields = Arrays.asList("field1", "field2", "field0");
        assertConstructorGettersSetters(projectedFields);        

        projectedFields = Arrays.asList(SchemaConstants._ID, SchemaConstants.PAYLOAD, SchemaConstants.SPAN_LIST);
        assertConstructorGettersSetters(projectedFields);
    }
    
    /**
     * Assert the correctness of the Constructor, getter and setter methods.
     * @param projectedFields The list of projected fields to be tested.
     */
    private void assertConstructorGettersSetters(List<String> projectedFields){
        SelectSomeFieldsPredicate selectSomeFieldsPredicate;
        
        // Check constructor
        selectSomeFieldsPredicate = new SelectSomeFieldsPredicate(projectedFields);
        Assert.assertEquals(selectSomeFieldsPredicate.getProjectedFields(), projectedFields);
        
        // Check set projectedFields to null
        selectSomeFieldsPredicate.setProjectedFields(null);
        Assert.assertEquals(selectSomeFieldsPredicate.getProjectedFields(), null);
        
        // Check set projectedFields to the given list of fields
        selectSomeFieldsPredicate.setProjectedFields(projectedFields);
        Assert.assertEquals(selectSomeFieldsPredicate.getProjectedFields(), projectedFields);
    }

    /**
     * Test the generateOperatorBean method.
     * Build a SelectSomeFieldsPredicate, invoke the generateOperatorBean and check
     * whether a ProjectionBean with the right attributes is returned.
     * An empty list is used as the list of projected fields.
     */
    @Test
    public void testGenerateOperatorBean00() {
        String operatorId = "xxx";
        List<String> projectedFields = Collections.emptyList();
        SelectSomeFieldsPredicate selectSomeFieldsPredicate = new SelectSomeFieldsPredicate(projectedFields);
        
        OperatorBean computedProjectionBean = selectSomeFieldsPredicate.generateOperatorBean(operatorId);
        OperatorBean expectedProjectionBean = new ProjectionBean(operatorId, "Projection", "", null, null);
        
        Assert.assertEquals(expectedProjectionBean, computedProjectionBean);
    }
    
    /**
     * Test the generateOperatorBean method.
     * Build a SelectSomeFieldsPredicate, invoke the generateOperatorBean and check
     * whether a ProjectionBean with the right attributes is returned.
     * A list with some field names is used as the list of projected fields.
     */
    @Test
    public void testGenerateOperatorBean01() {
        String operatorId = "zwx";
        List<String> projectedFields = Arrays.asList("field0", "field1");
        SelectSomeFieldsPredicate selectSomeFieldsPredicate = new SelectSomeFieldsPredicate(projectedFields);
        
        OperatorBean computedProjectionBean = selectSomeFieldsPredicate.generateOperatorBean(operatorId);
        OperatorBean expectedProjectionBean = new ProjectionBean(operatorId, "Projection", "field0,field1", null, null);

        Assert.assertEquals(expectedProjectionBean, computedProjectionBean);        
    }

    /**
     * Test the generateOperatorBean method.
     * Build a SelectSomeFieldsPredicate, invoke the generateOperatorBean and check
     * whether a ProjectionBean with the right attributes is returned.
     * A list with some unordered field names is used as the list of projected fields.
     */
    @Test
    public void testGenerateOperatorBean02() {
        String operatorId = "op00";
        List<String> projectedFields = Arrays.asList("c", "a", "b");
        SelectSomeFieldsPredicate selectSomeFieldsPredicate = new SelectSomeFieldsPredicate(projectedFields);
        
        OperatorBean computedProjectionBean = selectSomeFieldsPredicate.generateOperatorBean(operatorId);
        OperatorBean expectedProjectionBean = new ProjectionBean(operatorId, "Projection", "c,a,b", null, null);
        
        Assert.assertEquals(expectedProjectionBean, computedProjectionBean);   
    }

    /**
     * Test the generateOutputSchema method.
     * This test use an empty Schema as input and no fields to be projected.
     * The expected output Schema is an empty schema.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema00() throws TextDBException {        
        List<String> projectedFields = Collections.emptyList();
        SelectSomeFieldsPredicate selectSomeFieldsPredicate = new SelectSomeFieldsPredicate(projectedFields);
        
        Schema inputSchema = new Schema();
        
        Schema computedOutputSchema = selectSomeFieldsPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = new Schema();
        
        Assert.assertEquals(expectedOutputSchema ,computedOutputSchema);
    }

    /**
     * Test the generateOutputSchema method.
     * This test use a Schema with attributes with all values of FieldType
     * and some fields to be projected in the same order which they are in
     * the input schema ("fieldId", "fieldDouble", "fieldString", "fieldList").
     * The expected result is an output schema with the attributes present
     * in the list of fields to be projected.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema01() throws TextDBException {
        List<String> projectedFields = Arrays.asList(
                StatementTestUtils.ID_ATTRIBUTE.getFieldName(),
                StatementTestUtils.DOUBLE_ATTRIBUTE.getFieldName(),
                StatementTestUtils.STRING_ATTRIBUTE.getFieldName(),
                StatementTestUtils.LIST_ATTRIBUTE.getFieldName()
            );
        SelectSomeFieldsPredicate selectSomeFieldsPredicate = new SelectSomeFieldsPredicate(projectedFields);
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
        
        Schema computedOutputSchema = selectSomeFieldsPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = new Schema(
                StatementTestUtils.ID_ATTRIBUTE,
                StatementTestUtils.DOUBLE_ATTRIBUTE,
                StatementTestUtils.STRING_ATTRIBUTE,
                StatementTestUtils.LIST_ATTRIBUTE
            );
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }
    
    /**
     * Test the generateOutputSchema method.
     * This test use a Schema with attributes with all values of FieldType
     * and some fields to be projected in a different order in which they 
     * are in the input schema("fieldList", "fieldDouble", "fieldString",
     * "fieldId").
     * The expected result is an output schema with the attributes present
     * in the list of fields to be projected.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema02() throws TextDBException {
        List<String> projectedFields = Arrays.asList(
                StatementTestUtils.LIST_ATTRIBUTE.getFieldName(),
                StatementTestUtils.DOUBLE_ATTRIBUTE.getFieldName(),
                StatementTestUtils.STRING_ATTRIBUTE.getFieldName(),
                StatementTestUtils.ID_ATTRIBUTE.getFieldName()
            );
        SelectSomeFieldsPredicate selectSomeFieldsPredicate = new SelectSomeFieldsPredicate(projectedFields);
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
        
        Schema computedOutputSchema = selectSomeFieldsPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = new Schema(
                StatementTestUtils.ID_ATTRIBUTE,
                StatementTestUtils.DOUBLE_ATTRIBUTE,
                StatementTestUtils.STRING_ATTRIBUTE,
                StatementTestUtils.LIST_ATTRIBUTE
            );
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }

    /**
     * Test the generateOutputSchema method.
     * This test use a Schema with attributes with all values of FieldType
     * and no fields to be projected.
     * The expected output Schema is an empty schema.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema03() throws TextDBException {
        List<String> projectedFields = Collections.emptyList();
        SelectSomeFieldsPredicate selectSomeFieldsPredicate = new SelectSomeFieldsPredicate(projectedFields);
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
        
        Schema computedOutputSchema = selectSomeFieldsPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = new Schema();
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }

    /**
     * Test the generateOutputSchema method.
     * This test use a Schema with attributes with all values of FieldType
     * and all attributes in the list of fields to be projected.
     * The expected result is an output schema just like the input schema.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema04() throws TextDBException {
        List<String> projectedFields = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA.getAttributeNames();
        SelectSomeFieldsPredicate selectSomeFieldsPredicate = new SelectSomeFieldsPredicate(projectedFields);
        
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
        
        Schema computedOutputSchema = selectSomeFieldsPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }
    
    /**
     * Test the generateOutputSchema method.
     * This test use an empty Schema as input and field 'x' to be projected.
     * The expected result is a TextDBException being thrown, since attribute 
     * 'x' does not exist in the input schema.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test(expected = TextDBException.class)
    public void testGenerateOutputSchema05() throws TextDBException {
        List<String> projectedFields = Arrays.asList("x");
        SelectSomeFieldsPredicate selectSomeFieldsPredicate = new SelectSomeFieldsPredicate(projectedFields); 
        Schema inputSchema = new Schema();
        
        selectSomeFieldsPredicate.generateOutputSchema(inputSchema);
    }
    
    /**
     * Test the generateOutputSchema method.
     * This test use a Schema with attributes with all values of FieldType
     * and a list with valid field names and an invalid field name.
     * The expected result is a TextDBException being thrown, since field
     * 'fieldInvalid' does not exist in the input schema.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test(expected = TextDBException.class)
    public void testGenerateOutputSchema06() throws TextDBException {
        List<String> projectedFields = Arrays.asList(
                StatementTestUtils.LIST_ATTRIBUTE.getFieldName(),
                StatementTestUtils.DOUBLE_ATTRIBUTE.getFieldName(),
                "fieldInvalid",
                StatementTestUtils.STRING_ATTRIBUTE.getFieldName(),
                StatementTestUtils.ID_ATTRIBUTE.getFieldName()
            );
        SelectSomeFieldsPredicate selectSomeFieldsPredicate = new SelectSomeFieldsPredicate(projectedFields);
        
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
        
        selectSomeFieldsPredicate.generateOutputSchema(inputSchema);
    }
    
    /**
     * Test the generateOutputSchema method.
     * This test use a Schema with attributes with all values of FieldType
     * and a list with valid field names, ignoring duplicate names.
     * The expected output Schema is an output schema with multiples
     * attributes including the duplicates attributes.
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema07() throws TextDBException {
        List<String> projectedFields = Arrays.asList(
                StatementTestUtils.LIST_ATTRIBUTE.getFieldName(),
                StatementTestUtils.DOUBLE_ATTRIBUTE.getFieldName(),
                StatementTestUtils.LIST_ATTRIBUTE.getFieldName(),
                StatementTestUtils.STRING_ATTRIBUTE.getFieldName(),
                StatementTestUtils.ID_ATTRIBUTE.getFieldName()
            );
        SelectSomeFieldsPredicate selectSomeFieldsPredicate = new SelectSomeFieldsPredicate(projectedFields);
        
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
        
        Schema computedOutputSchema = selectSomeFieldsPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = new Schema(
                StatementTestUtils.ID_ATTRIBUTE,
                StatementTestUtils.DOUBLE_ATTRIBUTE,
                StatementTestUtils.STRING_ATTRIBUTE,
                StatementTestUtils.LIST_ATTRIBUTE
            );
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }

    /**
     * Test the generateOutputSchema method to check whether the name of
     * the projected fields are case-sensitive. 
     * This test use a Schema with all the attributes in SchemaConstants
     * and project some fields with valid name but with different
     * capitalization.
     * The expected output Schema is an output schema with the projected
     * fields (the capitalization of the name of the field should be the
     * same as the original Schema).
     * @throws TextDBException If an exception is thrown while generating
     *  the new Schema.
     */
    @Test
    public void testGenerateOutputSchema08() throws TextDBException {
        List<String> projectedFields = Arrays.asList(
                StatementTestUtils.LIST_ATTRIBUTE.getFieldName().toLowerCase(),
                StatementTestUtils.DOUBLE_ATTRIBUTE.getFieldName().toUpperCase(),
                StatementTestUtils.STRING_ATTRIBUTE.getFieldName().toUpperCase(),
                StatementTestUtils.ID_ATTRIBUTE.getFieldName().toLowerCase()
            );
        SelectSomeFieldsPredicate selectSomeFieldsPredicate = new SelectSomeFieldsPredicate(projectedFields);
        
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
        
        Schema computedOutputSchema = selectSomeFieldsPredicate.generateOutputSchema(inputSchema);
        Schema expectedOutputSchema = new Schema(
                StatementTestUtils.ID_ATTRIBUTE,
                StatementTestUtils.DOUBLE_ATTRIBUTE,
                StatementTestUtils.STRING_ATTRIBUTE,
                StatementTestUtils.LIST_ATTRIBUTE
            );
        
        Assert.assertEquals(expectedOutputSchema, computedOutputSchema);
    }
    
    /**
     * Integration test: assert the schemas obtained by invoking the
     * generateOutputSchema method in the predicate and getOutpuSchema in 
     * the generated operator are the same.
     * A schema with attributes of all types is used as the input schema
     * and a list with some fields ("fieldDate", "fieldDouble", 
     * "fieldString" and "fieldText") is used as the list of fields to 
     * be projected.
     */
    @Test
    public void integrationOutputSchemaTest00() throws TextDBException {
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
        SelectSomeFieldsPredicate selectSomeFieldsPredicate = new SelectSomeFieldsPredicate(Arrays.asList(
                StatementTestUtils.DATE_ATTRIBUTE.getFieldName().toUpperCase(),
                StatementTestUtils.DOUBLE_ATTRIBUTE.getFieldName().toUpperCase(),
                StatementTestUtils.STRING_ATTRIBUTE.getFieldName().toUpperCase(),
                StatementTestUtils.ID_ATTRIBUTE.getFieldName().toUpperCase()
            ));
        // Assert correct bean creation
        OperatorBean operatorBean = selectSomeFieldsPredicate.generateOperatorBean("xxx");
        Assert.assertTrue(operatorBean instanceof ProjectionBean);
        // Assert correct operator creation
        IOperator operator = PlanGenUtils.buildOperator(operatorBean.getOperatorType(), operatorBean.getOperatorProperties());
        Assert.assertTrue(operator instanceof ProjectionOperator);
        ProjectionOperator projectionOperator = (ProjectionOperator)operator;
        IOperator projectionInputOperator = StatementTestUtils.generateSampleIOperator(inputSchema, Collections.emptyList());
        projectionOperator.setInputOperator(projectionInputOperator);
        // Get schemas from statement and operator and assert they are the same
        Schema statementOutputSchema = selectSomeFieldsPredicate.generateOutputSchema(inputSchema);
        projectionOperator.open();
        while (projectionOperator.getNextTuple() != null) {
            // Some exceptions may be thrown only when the operator is iterated
        }
        Schema operatorOutputSchema = projectionOperator.getOutputSchema();
        projectionOperator.close();
        // Assert schemas from statement and from the operator are equal
        Assert.assertEquals(statementOutputSchema, operatorOutputSchema);
    }
    
    /**
     * Integration test: assert an exception is thrown when calling the
     * generateOutputSchema method of the predicate if and only if building
     * the operator by using edu.uci.ics.textdb.plangen.operatorbuilder.* 
     * and running it will also cause an exception to be thrown when
     * the same parameters are used.
     * This test check if an exception is thrown when a non existing field
     * or field with incompatible type is used.
     */
    @Test
    public void integrationExceptionDueToIncompatibleFieldTypeTest00(){
        Schema inputSchema = StatementTestUtils.ALL_FIELD_TYPES_SCHEMA;
        List<String> projectedFieldsToTest = inputSchema.getAttributeNames();
        projectedFieldsToTest.add("invalidField"); // Also test with non existing fields
        // Iterate over all Fields to be projected
        for(String projectedField : projectedFieldsToTest){
            Schema predicateOutputSchema=null, operatorOutputSchema=null;
            boolean predicateOutputSuccess, operatorOutputSuccess;
            // Build the predicate
            List<String> projectedFields = Arrays.asList(projectedField);
            SelectSomeFieldsPredicate selectSomeFieldsPredicate = new SelectSomeFieldsPredicate(projectedFields);
            // Get the output schema from the statement
            try{
                predicateOutputSchema = selectSomeFieldsPredicate.generateOutputSchema(inputSchema);
                predicateOutputSuccess = true;
            }catch(TextDBException e){
                predicateOutputSuccess = false;
            }
            // Get the output schema from the generated operator
            try{
                // Assert the bean creation
                String operatorBeanId = "xxx";
                OperatorBean operatorBean = selectSomeFieldsPredicate.generateOperatorBean(operatorBeanId);
                Assert.assertTrue(operatorBean instanceof ProjectionBean);
                // Assert the operator creation
                IOperator operator = PlanGenUtils.buildOperator(operatorBean.getOperatorType(), operatorBean.getOperatorProperties());
                Assert.assertTrue(operator instanceof ProjectionOperator);
                ProjectionOperator projectionOperator = (ProjectionOperator)operator;
                // Create an operator and set it as input of the KeywordMatcher operator
                IOperator keywordMatcherInputOperator = StatementTestUtils.generatePopulatedSampleOperator();
                Assert.assertEquals("Error in test code: defined input schema is different than the sample schema", 
                        keywordMatcherInputOperator.getOutputSchema(), inputSchema);
                projectionOperator.setInputOperator(keywordMatcherInputOperator);
                // Iterate and get schema from the operator 
                projectionOperator.open();
                while (projectionOperator.getNextTuple() != null) {
                    // Some exceptions may be thrown only when the operator is iterated
                }
                operatorOutputSchema = projectionOperator.getOutputSchema();
                projectionOperator.close();
                operatorOutputSuccess = true;
            }catch(TextDBException e){
                operatorOutputSuccess = false;
            }
            // Assert the result from statement and generated operator are the equal
            Assert.assertEquals("Expected predicate and operator to both fail or succeed", predicateOutputSuccess, operatorOutputSuccess);
            if(predicateOutputSuccess==true && operatorOutputSuccess==true){
                Assert.assertEquals("Output schema from predicate and operator does not match", predicateOutputSchema, operatorOutputSchema);   
            }
        }
    }
    
}
