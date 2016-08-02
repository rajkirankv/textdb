package edu.uci.ics.textdb.dataflow.sink;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;

/**
 * Created by chenli on 5/11/16.
 *
 * This class serializes each tuple from the subtree to a given file.
 */
public class FileSink extends AbstractSink {

    private PrintWriter printWriter;
    private final File file;
    private TupleToString toStringFunc;
    
    @FunctionalInterface
    public static interface TupleToString {
    	String convertToString(ITuple tuple);
    }
    
    public FileSink(IOperator childOperator, File file) throws FileNotFoundException {
        super(childOperator);
        this.file = file;
        this.toStringFunc = null;
    }

    public FileSink(IOperator childOperator, File file, TupleToString toStringFunc) throws FileNotFoundException {
        super(childOperator);
        this.file = file;
        this.toStringFunc = toStringFunc;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.printWriter = new PrintWriter(file);
    }

    @Override
    public void close() throws Exception {
        if (this.printWriter != null){
            this.printWriter.close();
        }
        super.close();
    }

    @Override
    protected void processOneTuple(ITuple nextTuple) {
    	if (this.toStringFunc == null) {
            printWriter.write(nextTuple.toString());
    	} else {
            printWriter.write(toStringFunc.convertToString(nextTuple));
    	}
    }
}
