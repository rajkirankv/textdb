package edu.uci.ics.textdb.dataflow.source;

import java.util.Iterator;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.ISourceOperator;

public class IterableSourceOperator implements ISourceOperator {

	@FunctionalInterface
	public static interface ToTuple {
		ITuple convertToTuple(String str) throws Exception;
	}
	
	private Iterable<String> iterableSource;
	private Iterator<String> iterator;
	private ToTuple toTupleFunc;
	
	private Schema schema;

	public IterableSourceOperator(Iterable<String> iterable, ToTuple toTupleFunc, Schema schema) {
		this.iterableSource = iterable;
		this.toTupleFunc = toTupleFunc;
	}

	@Override
	public void open() throws Exception {
		this.iterator = iterableSource.iterator();
	}

	@Override
	public ITuple getNextTuple() throws Exception {
		if (iterator.hasNext()) {
			try {
				return this.toTupleFunc.convertToTuple(iterator.next());		 
			} catch (Exception e) {
				e.printStackTrace(System.err);
				return getNextTuple();
			}
		}	
		return null;
	}

	@Override
	public void close() throws Exception {
		this.iterator = null;
	}

    @Override
    public Schema getOutputSchema() {
        return schema;
    }
}
