package edu.uci.ics.textdb.textql.statements.predicates;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.textql.planbuilder.beans.PassThroughBean;
import edu.uci.ics.textdb.web.request.beans.OperatorBean;

/**
 * Object representation of a "SELECT *" predicate inside a { @code SelectExtractStatement }.
 * 
 * @author Flavio Bayer
 *
 */
public class SelectAllFieldsPredicate implements SelectPredicate {

    /**
     * Return this operator converted to an { @code OperatorBean }.
     * @param selectOperatorId The ID of the OperatorBean to be created.
     */
    public OperatorBean generateOperatorBean(String selectOperatorId) {
        return new PassThroughBean(selectOperatorId, "PassThrough");
    }
    
    /**
     * Generate the resulting output schema of this predicate based on the given input schema.
     * This predicate does not apply any modification to the input schema, thus the generated
     * output schema is equal to the input schema.
     * @param inputSchema The input schema of this predicate.
     * @return The generated output schema based on the input schema.
     */
    public Schema generateOutputSchema(Schema inputSchema) {
        // Create a copy of the input schema and return it
        return new Schema(inputSchema.getAttributes().toArray(new Attribute[0]));
    }
    
    
    @Override
    public boolean equals(Object other) {
        if (other == null) { return false; }
        if (other.getClass() != getClass()) { return false; }
        SelectAllFieldsPredicate selectAllPredicate = (SelectAllFieldsPredicate) other;
        return true;
    }
}
