package freqitems;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import freqitems.OnlineRetailDebugging.ReadInFilter;
import freqitems.util.ItemSet;

/**
 * 
 * @author Ariane Ziehn
 *  
 * Run with parameters:
 *    ./src/main/resources/generated_data.csv 2 5
 */
public class BasketRefac {

	public static void main(String[] args) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		/**
		 * READ IN AND CLEAR DATA
		 */

		/*
		 * 1. add path of the data
		 */
		String dataPath =  args[0];
		int minSupport = Integer.parseInt(args[1]);
		int numIterations = Integer.parseInt(args[2]);

		/*
		 * 2. Check how your data is organzised e.g. :
		 * RPA_TNR|"PLANT"|"RPA_WID"|"RPA_BDD"|"RPA_TIX"|"VERSION"|"RPA_RFL"|...
		 */

		DataSet<Tuple2<String, String>> csvInput = env
				.readCsvFile(dataPath).fieldDelimiter(",")
				.includeFields("11000000").ignoreFirstLine()
				.types(String.class, String.class);
				

		/*
		 * We have created a Flink dataset from your input data file with the
		 * following informations: (TransactionID, ProduktID, TODO)
		 * 
		 * To fasten up the mining process in the next step we map the String
		 * ProduktID to an unique numerical identifier
		 */
		
		//Do not remove distinct 
		DataSet<Tuple2<Integer, String>> itemsSet = csvInput
				.distinct()
				.map(new AssignUniqueId())
				.setParallelism(1);
		System.out.println("\n****** Number of unique items:" +  itemsSet.count());
		/*
		 * In the next step we use dataset itemset above to create a List of
		 * Integers containing all items of one transaction first of all we
		 * create groups with the TransactionID using .group(0)and finally map
		 * the unique identifiers from itemSet to the String values from
		 * csvInput
		 */

		DataSet<List<Integer>> transactionList = csvInput
				.groupBy(0)
				.reduceGroup(new GroupItemsInListPerBill())
				.withBroadcastSet(itemsSet, "itemSet");
		System.out.println("\n****** Number of transactions:" +  transactionList.count());
		
//		List<List<Integer>> toWrite = transactionList.collect();
//		
//		File writeFile = new File("retailCSVInteger.csv");
//		FileWriter writer = new FileWriter(writeFile);
//
//		for (List<Integer> list : toWrite) {
//			for(Integer i : list){
//				writer.append(i+" ");		
//			}
//			writer.append("\n");			
//		}
//
//		writer.flush();
//		writer.close();
		
		
		/**
		 * MINING PART
		 */

		/*
		 * send transactions to Apriori with .mine(DataSet<List<Integer>,
		 * MinimumSupport, Iterations)
		 */
		DataSet<ItemSet> items = Apriori.mine(transactionList, minSupport, numIterations);

		/**
		 * POST PROCESSING
		 */

		/*
		 * For the final result the last transformation is the map back of the
		 * ProductID's
		 */

		DataSet<List<String>> transactionListMapped = items.flatMap(
				new MapBackTransactions1()).withBroadcastSet(itemsSet,
				"itemSet");

		transactionListMapped.print();

	} // end main method

	public static final class AssignUniqueId
			implements
			MapFunction<Tuple2<String, String>, Tuple2<Integer, String>> {

		private static final long serialVersionUID = 1L;
		int numItems = 1;

		@Override
		public Tuple2<Integer, String> map(Tuple2<String, String> value) {
			return new Tuple2<>(numItems++, value.f1);
		}
	}

	public static class GroupItemsInListPerBill
			extends
			RichGroupReduceFunction<Tuple2<String, String>, List<Integer>> {

		private static final long serialVersionUID = 1L;
		// get the set of items or products and create a hash map, so to map
		// product codes into integers
		private Collection<Tuple2<Integer, String>> itemSet; // these is the set
																// of possible
																// items or
																// products
		private Hashtable<String, Integer> itemSetMap = new Hashtable<String, Integer>();;

		@Override
		public void open(Configuration parameters) throws Exception {
			this.itemSet = getRuntimeContext().getBroadcastVariable("itemSet");
			// Tuple2<map integer, product code>
			for (Tuple2<Integer, String> item : itemSet) {
				// itemSetMap.put(key: product code, value: map integer);
				itemSetMap.put(item.f1, item.f0);
			}
			// System.out.println(itemSetMap.entrySet());
		}

		@Override
		public void reduce(
				Iterable<Tuple2<String, String>> transactions,
				Collector<List<Integer>> out) {

			// transactions contains all the transactions with the same id or
			// RPA_TNR number
			// collect here the products of all those transactions and map them
			// into integers
			List<Integer> list_items = new ArrayList<Integer>();

			// f0 f1 f2
			// Tuple3<transaction number, timestamp, product>
			for (Tuple2<String, String> trans : transactions) {
				if (itemSetMap.containsKey(trans.f1))
					list_items.add(itemSetMap.get(trans.f1));
				// System.out.println("   " + trans.f0 + " " + trans.f2 + " " +
				// itemSetMap.get(trans.f2));
			}
			// System.out.println("   ----------------------");
			Collections.sort(list_items);
			out.collect(list_items);
		}
	}

	public static class MapBackTransactions1 extends
			RichFlatMapFunction<ItemSet, List<String>> {

		// get the set of items or products and create a hash map, so to map
		// product codes into integers
		private Collection<Tuple2<Integer, String>> itemSet; // these is the set
																// of possible
																// items or
																// products
		// here create the Hash map the other way around to map back
		private Hashtable<Integer, String> itemSetMap = new Hashtable<Integer, String>();;

		@Override
		public void open(Configuration parameters) throws Exception {
			this.itemSet = getRuntimeContext().getBroadcastVariable("itemSet");
			// Tuple2<map integer, product code>
			for (Tuple2<Integer, String> item : itemSet) {
				// itemSetMap.put(key: product code, value: map integer);
				itemSetMap.put(item.f0, item.f1);
			}
		}

		@Override
		public void flatMap(ItemSet freqItems, Collector<List<String>> out) {

			List<String> list_items = new ArrayList<String>();
			// List<Integer> itemsList = freqItems.getItemSetList();
			for (Integer item : freqItems.getItemSetList()) {
				if (itemSetMap.containsKey(item))
					list_items.add(itemSetMap.get(item));
			}
			out.collect(list_items);

		}
	}
}

