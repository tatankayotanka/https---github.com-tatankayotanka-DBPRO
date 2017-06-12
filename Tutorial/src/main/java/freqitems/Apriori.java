package freqitems;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.aggregators.DoubleSumAggregator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.util.Collector;
import freqitems.util.ItemSet;
import freqitems.util.Trie;

/**
 * 
 * @author charfuelan, ziehn
 *
 */
public class Apriori {

	DataSet<List<Integer>> transactionList;
	int minSupport;
	int numIterations;
	int printResults = 1;

	public Apriori(DataSet<List<Integer>> transactionList, int minSupport,
			int numIterations) {
		this.transactionList = transactionList;
		this.minSupport = minSupport;
		this.numIterations = numIterations;
	}

	/**
	 * Generate itemSet given a data set a corresponding minSupport and max
	 * number of iterations
	 * 
	 * @param data
	 *            : transactions data base filename
	 * @param minSupport
	 *            : integer ex. 150
	 * @param numIterations
	 *            : integer ex. 10 or a big number, it will iterate until it can
	 *            not generate more frequent itemSets
	 * @param printResults
	 *            : integer 1 or 0 whether to print or not results (itemSets
	 *            lists... to avoid printing very long list)
	 * @throws Exception
	 */
	public static DataSet<ItemSet> mine(DataSet<List<Integer>> transactionList,
			int minSupport, int numIterations) throws Exception {

		int numIterationsLoop; // the first three iterations are done outside
								// the loop

		DataSet<Tuple2<Integer, Integer>> itemSetsIterationTuple2 = transactionList
				.flatMap(new TransactionSplitter()).groupBy(0).sum(1)
				.filter(new ItemSetFilterTuple2(minSupport));

		DataSet<ItemSet> singletons = itemSetsIterationTuple2
				.map(new ItemSetGenerator());

		if (numIterations == 1) {

			return singletons;
		} else {

			// PHASE 2: from here we start to use a Trie for candidate
			// generation and an iteration loop
			// for the moment it work just for 1, 2 or max 3 iterations, this
			// means actually up to 5 iterations
			// because the first two iterations are done out of the loop
			numIterationsLoop = numIterations - 1;

			IterativeDataSet<ItemSet> loop = singletons
					.iterate(numIterationsLoop);

			loop.registerAggregationConvergenceCriterion("empty",
					new DoubleSumAggregator(),
					new VerifyNumberOfFrequentItemSets());

			// broadcast the itemSets generated in the previous iteration to
			// create a Trie
			// Use the Trie to generate candidates and then count them
			DataSet<ItemSet> finalItemSetsIteration = transactionList
					.reduceGroup(new CandidateGenerationWithTrieCounting())
					.withBroadcastSet(loop, "itemSetsIteration")
					.groupBy(new KeySelectorGeneral())
					.reduce(new CountItemSetsGeneral())
					.filter(new ItemSetFilter(minSupport));

			DataSet<ItemSet> finalItemSets = loop
					.closeWith(finalItemSetsIteration);

			// if( printResults==1 ) {
			return finalItemSets;
			// System.out.println("More iterations results in jobManager log...");
			// } else {
			// System.out.println("\nFinal number of ItemSets (size " +
			// numIterations + "): " + finalItemSets.count());
			// System.out.println("More iterations results in jobManager log...");
			// }
		}

		// long estimatedTime = System.currentTimeMillis() - startTime;
		// System.out.print("\nEstimated time (sec): " + estimatedTime/1000.0);
		// System.out.println(" Max Num iterations = " + numIterations +
		// " minSupport = " + minSupport);
		// System.out.println("data: " + dataPath);

	}

	public static class CandidateGenerationWithTrieCounting extends
			RichGroupReduceFunction<List<Integer>, ItemSet> {

		private static final long serialVersionUID = 1L;
		int iteration = 1;
		private Collection<ItemSet> itemSetsIteration; // these are the itemsets
														// of previous iteration
														// used to create a Trie
		private Trie trie;
		// add aggregator for checking convergence, in this case when there is
		// no more frequent itemsets
		private DoubleSumAggregator agg = null;

		@Override
		public void open(Configuration parameters) throws Exception {

			// The first time it will receive itemSets of size 2, so the first
			// Trie will have two levels
			this.itemSetsIteration = getRuntimeContext().getBroadcastVariable(
					"itemSetsIteration");

			// check if current list of itemsets is empty
			this.agg = this.getIterationRuntimeContext()
					.getIterationAggregator("empty");
			this.agg.aggregate(itemSetsIteration.size());

			// with these itemsets generate the Trie
			iteration++;
			List<Integer> itemsList = new ArrayList<Integer>();
			System.out
					.println("***** itemSetsIteration RECEIVED IN LOOP frequent itemsets in Trie: "
							+ itemSetsIteration.size()
							+ "  iteration="
							+ iteration);
			trie = new Trie();
			for (ItemSet trans : itemSetsIteration) {
				// Add the two elements of the itemsets to a list so to create
				// easily the Trie
				// System.out.print("   ADDING TO TRIE: ");
				for (int item : trans.getItemSetList())
					itemsList.add(item);
				// System.out.println(itemsList.toString());
				trie.addTransaction(itemsList);
			}

		}

		@Override
		public void reduce(Iterable<List<Integer>> transactions,
				Collector<ItemSet> out) {

			// System.out.println("   >>iteration: " + iteration);
			int numLevels = iteration; // the number of iteration should be the
										// number of levels in the trie+1
			// walking the trie it will be added other level to the trie

			// firts add another level to the trie, with possible candidates
			for (List<Integer> trans : transactions)
				trie.walkTransactionCounting(trans, numLevels);

			// generate ItemSet from the leaves added to the trie including the
			// counts
			trie.collectFrequentItemSets(out);

		}
	}

	public static class CountItemSetsGeneral implements ReduceFunction<ItemSet> {

		private static final long serialVersionUID = 1L;

		@Override
		public ItemSet reduce(ItemSet value1, ItemSet value2) throws Exception {
			return new ItemSet(value1.itemset, value1.getCount()
					+ value2.getCount());
		}
	}

	public static final class ItemSetFilter implements FilterFunction<ItemSet> {

		private static final long serialVersionUID = 1L;
		Integer minSupport;

		public ItemSetFilter(int minSupport) {
			super();
			this.minSupport = minSupport;
		}

		@Override
		public boolean filter(ItemSet value) {
			// if(value.count >= minSupport)
			// System.out.println(value.toString());
			return value.getCount() >= minSupport;
		}
	}

	public static class KeySelectorGeneral implements
			KeySelector<ItemSet, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(ItemSet value) throws Exception {
			return value.getItemSetId();
		}
	}

	public static class VerifyNumberOfFrequentItemSets implements
			ConvergenceCriterion<DoubleValue> {

		private static final long serialVersionUID = 1L;

		@Override
		public boolean isConverged(int iteration, DoubleValue value) {
			if (iteration <= 1)
				return false;
			return (value.getValue() == 0);
		}
	}

	public static class TransactionSplitter implements
			FlatMapFunction<List<Integer>, Tuple2<Integer, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(List<Integer> transaction,
				Collector<Tuple2<Integer, Integer>> out) {
			for (Integer item : transaction) {
				out.collect(new Tuple2<Integer, Integer>(item, 1));
			}
		}
	}

	public static final class ItemSetGenerator implements
			MapFunction<Tuple2<Integer, Integer>, ItemSet> {

		private static final long serialVersionUID = 1L;

		@Override
		public ItemSet map(Tuple2<Integer, Integer> arg0) throws Exception {
			List<Integer> items = new ArrayList<Integer>();
			items.add(arg0.f0);
			ItemSet singleItem = new ItemSet(items, arg0.f1);

			return singleItem;
		}

	}

	public static final class ItemSetFilterTuple2 implements
			FilterFunction<Tuple2<Integer, Integer>> {

		private static final long serialVersionUID = 1L;
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
	
	private static DataSet<String> getTextDataSet(ExecutionEnvironment env, String dataPath) {
		
		if(dataPath != null) {
			// read the test.csv file from resources path
			return env.readTextFile(dataPath);
		} else {
			// get default test.csv test.csv data
			String[] Transactions = new String[] {
					"1 3 4",
					"2 3 5",
					"1 2 3 5",
					"2 5",
					"1 3 5",
				};
			return env.fromElements(Transactions);
		}
	}
	
    public static class LineSplitter implements FlatMapFunction<String, List<Integer>> {
        @Override
        public void flatMap(String line, Collector<List<Integer>> out) {
        	List<Integer> itemsList = new ArrayList<Integer>();
			
            for (String word : line.split(" ")) {  
            	Integer i = Integer.parseInt(word);
            	if(!itemsList.contains(i))
            	itemsList.add(i);
            }         
            Collections.sort(itemsList); // sorting the list of items in each transaction
            out.collect(itemsList);            
        }
    }
	
	/**
	 * Generate itemSet given a data set a corresponding minSupport and max number of iterations
	 * @param data: transactions data base filename
	 * @param minSupport: integer ex. 150
     * @param numIterations: integer ex. 10 or a big number, it will iterate until it can not generate more frequent itemSets
     * @param printResults: integer 1 or 0 whether to print or not results (itemSets lists... to avoid printing very long list)
     * Run with parameters:
     *    ./src/main/resources/T10I4D100K.dat 150 10 4
     *    ./src/main/resources/simple_data.dat 2 3 4
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
	    	
	 	String dataPath = args[0];
		int minSupport = Integer.parseInt(args[1]);
		int numIterations = Integer.parseInt(args[2]);
		int numParallelism = Integer.parseInt(args[3]);
	    	
	    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	    env.setParallelism(numParallelism);
	        	        
	    // get input data
	    DataSet<String> text = getTextDataSet(env, dataPath);
	        
	    // get the list of transactions, every transaction will be kept in a List<Integer>
	    DataSet<List<Integer>> transactionList = text.flatMap(new LineSplitter());	         
	    System.out.println("Initial transactionList = " + transactionList.count());
	    
	    DataSet<ItemSet> items = Apriori.mine(transactionList, minSupport, numIterations);
	    
	    items.print();
	    
	    
	}

}
