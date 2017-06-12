package algorithms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import model.Transaction;
import util.AprioriData;


/**
 * this version has a very inefficient way of generating itemsets of size 3
 * @author marcela
 *
 */
public class Rapriori1 {
	private static int numTransactions = 0;

	/**
	 * use: apriori inputFile minSupport
	 * NOTE: works for testing in eclipse for files in resources only...
	 *   simple_data.txt 2
	 *   mushroom_short.dat 100
	 *   mushroom.dat 6000
	 *   pumsb.dat 47500
	 *   If running on the command line, use:
	 *      in /path-to/freqitems/mvn clean install
	 *   start flink jobManager: 
	 *      /path-to/flink/bin/start-local.sh
	 *   then on the command line:
	 *      /path-to/bin/flink run --class org.apache.flink.examples.java.freqitemsets.Apriori 
	 *      /path-to/freqitemsets/target/freqitemsets-1.0-SNAPSHOT.jar 
	 *      /path-to/freqitemsets/src/main/resources/mushroom_short.dat 100
	 *    
	 *   Example:   
	 *    /projects/Flink/flink-0.9.0/bin/flink run --class org.apache.flink.examples.java.freqitemsets.Apriori target/freqitemsets-1.0-SNAPSHOT.jar /projects/Flink/freqitemsets/src/main/resources/mushroom_short.dat 100
	 * @param args
	 * @throws Exception
	 */
    public static void main(String[] args) throws Exception {
    	int nPasses = 3;
    	//int minSupport = 2;
    	//String inputFile = "simple_data.txt";    	
    	//String inputFile = "mushroom_short.dat";    	
    	//String dataPath = Resources.getResource(inputFile).getFile();
    	
       	//int minSupport = 2;
    	//String dataPath = "/projects/data/FrequentItemsetMining/simple_data.dat";
     	
    	//int minSupport = 1000;
    	//String dataPath = "/projects/data/FrequentItemsetMining/T10I4D100K.dat";
    	
    	//int minSupport = 5940;
    	//String dataPath = "/projects/data/FrequentItemsetMining/kosarak.dat";
    	   	
    	int minSupport = 2100;
    	String dataPath = "./src/main/resources/T10I4D100K.dat";
    	
    	boolean discard = true;
    	
    	if(args.length > 0){
    		dataPath = args[0];
    		minSupport = Integer.parseInt(args[1]);
    		discard = Boolean.valueOf(args[2]);
    	} 
    	//System.out.println("******DATA:" + dataPath);
    	
    	long startTime = System.currentTimeMillis();
    	
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
     		
        // get input data
        DataSet<String> text = getTextDataSet(env, dataPath);
        
        // get the list of transactions, every transaction will be kept in a List<Integer>
        DataSet<List<Integer>> transactionList = text.flatMap(new LineSplitter());
        
        DataSet<Tuple3<Integer, Integer, Integer>> firstIteration;
        
        if(discard) {
        	// get the count of every single item, and keep just the ones above minSupport
        	DataSet<Integer> itemCounts = transactionList
        		                .flatMap(new transactionSplitter())  
        		                .groupBy(0)
                                .sum(1)
                                .filter(new ItemSetFilterTuple2(minSupport))
                                .sortPartition(0, Order.ASCENDING)  //TODO: check in which cases this might help
                                .map(new FieldSelector());
        	itemCounts.print();
                                         
        	// first iteration generate itemsets of size 2                	
        	firstIteration = transactionList
 		           .flatMap(new CandidateGeneration()).withBroadcastSet(itemCounts, "itemCounts")
 		           .groupBy(0, 1)
 	               .sum(2)
 		           .filter(new ItemSetFilterTuple3(minSupport));        	        	        	
        } else {
        	firstIteration = transactionList
		           .flatMap(new CandidateGenerationWithoutDiscarding())
		           .groupBy(0, 1)
	               .sum(2)
		           .filter(new ItemSetFilterTuple3(minSupport));        	
        }
        firstIteration.print();
        
        // from here I work with strings
        DataSet<String> items2Counts = firstIteration
                    .map(new FieldSelectorAndConverter());
       
        items2Counts.print();
     
        // second iteration generate itemsets of size 3                	
    	DataSet<Tuple4<Integer, Integer, Integer, Integer>> secondIteration = transactionList
		           .flatMap(new CandidateGeneration3()).withBroadcastSet(items2Counts, "items2Counts")
		           .groupBy(0, 1, 2)
	               .sum(3)
		           .filter(new ItemSetFilterTuple4(minSupport));
    	
    	secondIteration.print();
    	
        
                
 	    long estimatedTime = System.currentTimeMillis() - startTime;
 	    System.out.println("Estimated time (sec): " + estimatedTime/1000.0);
 	    if(discard){
 	    	System.out.println("Discarding non-frequent items from transactions");
 	    }
        
    }
    
    public static class LineSplitter implements FlatMapFunction<String, List<Integer>> {
        @Override
        public void flatMap(String line, Collector<List<Integer>> out) {
        	List<Integer> itemsList = new ArrayList<Integer>();
			
            for (String word : line.split(" ")) {            
            	itemsList.add(Integer.parseInt(word));
            }
          //TODO: check in which cases this might help
            Collections.sort(itemsList); // sorting the list of items in each transaction
            out.collect(itemsList);            
        }
    }
    
    public static class transactionSplitter implements FlatMapFunction<List<Integer>, Tuple2<Integer, Integer>> {
        @Override
        public void flatMap(List<Integer> transaction, Collector<Tuple2<Integer, Integer>> out) {
            for (Integer item : transaction) {
                out.collect(new Tuple2<Integer, Integer>(item, 1));
            }
        }
    }
    
  /*  .map(new MapFunction<Tuple2<Integer,Integer>, Integer>() {
        public Integer map(Tuple2<Integer,Integer> value) { return value.f0; }
   });  */  
    public static final class FieldSelector implements MapFunction<Tuple2<Integer, Integer>, Integer> {
        @Override
        public Integer map(Tuple2<Integer, Integer> value) {           
                return value.f0;            
        }
    }
    
    public static final class FieldSelectorAndConverter implements MapFunction<Tuple3<Integer, Integer, Integer>, String> {
        @Override
        public String map(Tuple3<Integer, Integer, Integer> value) {           
                return value.f0.toString() + value.f1.toString();            
        }
    }
    
	public static class CandidateGenerationWithoutDiscarding implements FlatMapFunction<List<Integer>, Tuple3<Integer, Integer, Integer>> {		
		@Override
		public void flatMap(List<Integer> list_items, Collector <Tuple3<Integer, Integer, Integer>> out) {

			// I need to generate combinations of items in each transaction
		    // generate itemsets of size 2
			for(int i=0; i<list_items.size()-1; i++){
				for(int j=i+1; j<list_items.size(); j++){					
					//System.out.println(" " + list_items.get(i) + " " + list_items.get(j));
					out.collect(new Tuple3<Integer, Integer, Integer>(list_items.get(i), list_items.get(j), 1));
				}				
			}						   			
		}
	}

	public static class CandidateGeneration3 extends RichFlatMapFunction<List<Integer>, Tuple4<Integer, Integer, Integer, Integer>> {	
		
		private Collection<Integer> items2Counts;

		@Override
		public void open(Configuration parameters) throws Exception {
			this.items2Counts = getRuntimeContext().getBroadcastVariable("items2Counts");
		}

		@Override
		public void flatMap(List<Integer> transaction, Collector <Tuple4<Integer, Integer, Integer, Integer>> out) {

			// I need to generate combinations of items in each transaction, but just with the items that
			// appear in itemCounts
			// at some point it would be good to sort every list of items, is it useful??			
			//System.out.println("transaction.size()= " + transaction.size());
			
			if(transaction.size() > 2 ){
				
				// here I create another transaction removing the pairs of items that are not in item2Counts
				// in order to check I make conversion to string, this might be very expensive and heavy
				List<Integer> list_items = new ArrayList<Integer>();
			
				for(int i=0; i<transaction.size()-1; i++){				
					for(int j=i+1; j<transaction.size(); j++){								
						// check if the pair of items appears in items2Count						
						if( items2Counts.contains( transaction.get(i).toString()+transaction.get(j).toString()) ){
						//	System.out.println("found: " + transaction.get(i) + " " + transaction.get(j));
							if(!list_items.contains(transaction.get(i)))
								list_items.add(transaction.get(i));
							if(!list_items.contains(transaction.get(j)))
								list_items.add(transaction.get(j));
						}
					}
				}
				
				/*for(int i=0; i<list_items.size(); i++){
					System.out.print(list_items.get(i) + " ");
				}
				System.out.println();
				*/
		
				if(list_items.size() > 2 ){
					// generate itemsets of size 3 and add 1 at the end so we can count them afterwards...
					for(int i=0; i<list_items.size()-2; i++){				
						for(int j=i+1; j<list_items.size()-1; j++){	
							for(int k=j+1; k<list_items.size(); k++) {
								//System.out.println(" " + list_items.get(i) + " " + list_items.get(j) + " " + list_items.get(k));
								out.collect(new Tuple4<Integer, Integer, Integer, Integer>
						             (list_items.get(i), list_items.get(j), list_items.get(k), 1));
							}
						}
					}
				}
			}  // if transaction length >= 3
		}
	}

	
	
	
	public static class CandidateGeneration extends RichFlatMapFunction<List<Integer>, Tuple3<Integer, Integer, Integer>> {	
		
		private Collection<Integer> itemCounts;

		@Override
		public void open(Configuration parameters) throws Exception {
			this.itemCounts = getRuntimeContext().getBroadcastVariable("itemCounts");
		}

		@Override
		public void flatMap(List<Integer> transaction, Collector <Tuple3<Integer, Integer, Integer>> out) {

			// I need to generate combinations of items in each transaction, but just with the items that
			// appear in itemCounts
			// at some point it would be good to sort every list of items, is it useful??			

			// here I create another transaction removing the items that are not frequent
			List<Integer> list_items = new ArrayList<Integer>();
			for(Integer item : transaction){
				if( itemCounts.contains(item))
					list_items.add(item);
			}

			// generate itemsets of size 2
			for(int i=0; i<list_items.size()-1; i++){				
					for(int j=i+1; j<list_items.size(); j++){						
						//System.out.println(" " + list_items.get(i) + " " + list_items.get(j));
						out.collect(new Tuple3<Integer, Integer, Integer>(list_items.get(i), list_items.get(j), 1));
					}
			}						   			
		}
		
/*
			// generate itemsets of size 2
			for(int i=0; i<transaction.size()-1; i++){				
				if(itemCounts.contains(transaction.get(i))){
					for(int j=i+1; j<transaction.size(); j++){						
						//System.out.println(" " + list_items.get(i) + " " + list_items.get(j));
						if(itemCounts.contains(transaction.get(j))){
						out.collect(new Tuple3<Integer, Integer, Integer>(transaction.get(i), transaction.get(j), 1));
						}
					}
				}
			}						   			
		}
		*/
	}

    /**
     * This function is slower that the version with RichFlatMap...
     */
	public static final class CandidateGenerationGroupReduce extends
	                          RichGroupReduceFunction<List<Integer>, Tuple3<Integer, Integer, Integer>> {		
        private Collection<Integer> itemCounts;
		
		@Override
		public void open(Configuration parameters) throws Exception {
			this.itemCounts = getRuntimeContext().getBroadcastVariable("itemCounts");
		}

		@Override
		public void reduce(Iterable<List<Integer>> trans, Collector <Tuple3<Integer, Integer, Integer>> out) {
			
			// I need to generate combinations of items in each transaction, but just with the items that
			// appear in itemCounts
			for (List<Integer> t : trans) {
				
				// at some point it would be good to sort every list of items, is it useful??
				List<Integer> list_items = new ArrayList<Integer>();
				
				// here I create another transaction removing the items that are not frequent
				for(Integer item : t){
					if( itemCounts.contains(item))
						list_items.add(item);
				}
			    //TODO here is the RAPRIORI IDEA implemented !!!!
				// generate itemsets of size 2
				for(int i=0; i<list_items.size()-1; i++){
					for(int j=i+1; j<list_items.size(); j++){
					  //String newItemSet = list_items.get(i) + " " + list_items.get(j);
					  //System.out.println(" " + list_items.get(i) + " " + list_items.get(j));
					  //out.collect(new Tuple2<String, Integer>(newItemSet, 1));
					  out.collect(new Tuple3<Integer, Integer, Integer>(list_items.get(i), list_items.get(j), 1));
					}				
				}						   
			}
			
		}
	}


	
	
	private static DataSet<String> getTextDataSet(ExecutionEnvironment env, String dataPath) {
		
		if(dataPath != null) {
			// read the text file from resources path
			return env.readTextFile(dataPath);
		} else {
			// get default test text data
			return AprioriData.getDefaultTextLineDataSet(env);
		}
	}
    
 
    
    public static final class ItemSetFilterTuple2 implements FilterFunction<Tuple2<Integer, Integer>> {
        Integer minSupport;

        public ItemSetFilterTuple2(int minSupport) {
            super();
            this.minSupport = minSupport;
        }

        @Override
        public boolean filter(Tuple2<Integer, Integer> value) {
            return value.f1 >= minSupport;
        }
    }

    public static final class ItemSetFilterTuple3 implements FilterFunction<Tuple3<Integer, Integer, Integer>> {
        Integer minSupport;

        public ItemSetFilterTuple3(int minSupport) {
            super();
            this.minSupport = minSupport;
        }

        @Override
        public boolean filter(Tuple3<Integer, Integer, Integer> value) {
            return value.f2 >= minSupport;
        }
    }    
    
    public static final class ItemSetFilterTuple4 implements FilterFunction<Tuple4<Integer, Integer, Integer, Integer>> {
        Integer minSupport;

        public ItemSetFilterTuple4(int minSupport) {
            super();
            this.minSupport = minSupport;
        }

        @Override
        public boolean filter(Tuple4<Integer, Integer, Integer, Integer> value) {
            return value.f3 >= minSupport;
        }
    }    
    
    public static final class ItemFilter implements FilterFunction<Tuple2<Integer, Integer>> {
        Integer minSupport;

        public ItemFilter(int minSupport) {
            super();
            this.minSupport = minSupport;
        }

        @Override
        public boolean filter(Tuple2<Integer, Integer> value) {
            return value.f1 >= minSupport;
        }
    }
}