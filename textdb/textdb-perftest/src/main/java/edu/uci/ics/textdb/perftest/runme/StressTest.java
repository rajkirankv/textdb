package edu.uci.ics.textdb.perftest.runme;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.perftest.keywordmatcher.KeywordMatcherPerformanceTest;
import edu.uci.ics.textdb.perftest.utils.PerfTestUtils;


/**
 * @author Qinhua Huang, Hailey Pan
 */


public class StressTest {

	/**
	 * @param args
	 */
    /*
     * Test the performance of index creating and query 
     * using varied size of generated data sets.
     * The size and time-spended will be collected in resultFile.
     * 
     * Arguments: 
     * None
     * 
     * Need to set the local data set path for origData.
     * 
     * output:
     * indextimeSize.csv
     * 
     * 
     */
    public static String resultFile = "indexTimeSize.csv";
    public static String delimiter = ",";
    public static String newLine = "\n";
	public static double writeStandardAnalyzerIndexTimeCost = 0.0;
	public static double writeTrigramIndicesTimeCost = 0.0;
	public static String origData = "/home/sjwn/uci_data/abstract_1M.txt";
    
    public static double getDirSize(File file) {          
        if (file.exists()) {     
            if (file.isDirectory()) {     
                File[] children = file.listFiles();     
                double size = 0;     
                for (File f : children){     
                    size += getDirSize(f);
                }
                return size;     
            }
            else {   
                double size = file.length();        
                return size;     
            }     
        }
        else {     
            System.out.println("File do not exist!");     
            return 0;     
        }     
    }
    public static void runStressTest(){

        try {
            PerfTestUtils.deleteDirectory(new File(PerfTestUtils.standardIndexFolder));
            PerfTestUtils.deleteDirectory(new File(PerfTestUtils.trigramIndexFolder));
            
            long startTime;
            long endTime;
            
          //Start write standAnalyzeindices
            startTime=System.currentTimeMillis();   //StartTime
            PerfTestUtils.writeStandardAnalyzerIndices();
            endTime=System.currentTimeMillis(); //EndTime
            writeStandardAnalyzerIndexTimeCost = (endTime-startTime)/1000.0;
            long sizeStandardAnalyzerIndices =(long) getDirSize(new File(PerfTestUtils.standardIndexFolder))/1024;
            
          //Start write Trigram Index
            startTime=System.currentTimeMillis(); 
            PerfTestUtils.writeTrigramIndices();
            endTime=System.currentTimeMillis();
            writeTrigramIndicesTimeCost = (endTime-startTime)/1000.0;
            long sizeTrigramIndices = (long) getDirSize(new File(PerfTestUtils.trigramIndexFolder))/1024;

            List<Double> thresholds = Arrays.asList(0.8, 0.65, 0.5, 0.35);
            List<String> regexQueries = Arrays.asList("mosquitos?", "v[ir]{2}[us]{2}", "market(ing)?",
                    "medic(ine|al|ation|are|aid)?", "[A-Z][aeiou|AEIOU][A-Za-z]*");

            //Start Query
            KeywordMatcherPerformanceTest.runTest("sample_queries.txt");
  //          DictionaryMatcherPerformanceTest.runTest("sample_queries.txt");
  //          FuzzyTokenMatcherPerformanceTest.runTest("sample_queries.txt", thresholds);
  //          RegexMatcherPerformanceTest.runTest(regexQueries);
  //          NlpExtractorPerformanceTest.runTest();
            
            //output size and time spend.
            String HEADER = "dataSrcName, timeWriteStdAnaIndex, sizeStdAnaIndex,timeWriteTrigIndex, sizeTrigIndex ";
            PerfTestUtils.createFile(PerfTestUtils.getResultPath(resultFile), HEADER);
            FileWriter fileWriter = new FileWriter(PerfTestUtils.getResultPath(resultFile), true);
            fileWriter.append(newLine);
            fileWriter.append(PerfTestUtils.srcFileName +delimiter + writeStandardAnalyzerIndexTimeCost + delimiter + 
            		 sizeStandardAnalyzerIndices + delimiter + writeTrigramIndicesTimeCost + delimiter + sizeTrigramIndices); 
            fileWriter.flush();
            fileWriter.close();

        }
        catch (StorageException | DataFlowException | IOException e) {
            e.printStackTrace();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return;
    }
        
    public static void main(String[] args) throws IOException{
    	//size of data sets
    	int [] tests = {10,100,1000,10000,100000,200000,300000,400000,500000,600000,700000,800000,900000,1000000};
    	
        try {
        	for (int i = 0; i<tests.length; i++){
	        	System.out.print("Start Test "+i+" !\n");
	            System.out.println("Generated: "+PerfTestUtils.genData(origData,tests[i])+" data!\n");
	            runStressTest();
        	}
        	System.out.print("Tests Ended!");
        }
        catch(ArrayIndexOutOfBoundsException e){
            e.printStackTrace();
        }
    }

}