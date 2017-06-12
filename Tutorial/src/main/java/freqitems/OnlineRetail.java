package freqitems;

import freqitems.util.ItemSet;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Ariane Ziehn
 *         <p>
 *         Run with:
 *         ./OnlineRetail/OnlineRetail.csv 2 5
 */
public class OnlineRetail {

	public static void main(String[] args) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		/**
		 * READ IN AND CLEAR DATA
		 */

		/*
         * 1. add path of the data
		 */
		String dataPath = args[0];
		int minSupport = Integer.parseInt(args[1]);
		int numIterations = Integer.parseInt(args[2]);

//		String dataPath = "/projects/Flink/dfki-tutorial/OnlineRetail/OnlineRetail.csv";

		/*
		 * 2. Check how your data is organzised:
		 * InvoiceNo,StockCode,Description,
		 * Quantity,InvoiceDate,UnitPrice,CustomerID,Country we have 8 cloumns >
		 * .includeFields("00000000") the seperator is "," >
		 * .fieldDelimiter(",") which columns are interesting? 1, 2 and 5 > >
		 * .includeFields("11001000") ATTENTION: in this data certain rows
		 * contains missings,that is why we need to filter
		 */

		DataSet<Tuple2<String, String>> csvInput = env
				.readCsvFile(dataPath).fieldDelimiter(",")
				.includeFields("110000000").ignoreFirstLine()
				.types(String.class, String.class)
				.filter(new ReadInFilter2());

		System.out.println("csvInput.count=" + csvInput.count());

		/*
		 * We have created a Flink dataset from the input OnlineRetail with the
		 * following informations: (InvoiceNo, StockCode, InvoiceDate)
		 *
		 * map the String StockCode to an unique numerical identifier we want
		 * all StockCodes distinct > .distinct(1) otherwise it is possible to
		 * re-use the methods from Basket Analysis, but need to check if we use
		 * the right indices
		 */
		DataSet<Tuple2<Integer, String>> itemsSet = csvInput.distinct(1)
				.map(new AssignUniqueId2()).setParallelism(1);

		System.out.println("\n****** Number of unique items:" + itemsSet.count());





		/*
		 * In the next step we use dataset itemset above to create a List of
		 * Integers containing all items of one transaction in a time sequence
		 * order groups of InvoiceNo > .group(0) to map the unique identifiers
		 * from itemSet to the String values from csvInput we can use the method
		 * from BasketAnalysis, but need to edit it for time relevant sorting
		 */

		DataSet<List<Integer>> transactionList = csvInput.groupBy(0)
				.reduceGroup(new GroupItemsInListPerBill2())
				.withBroadcastSet(itemsSet, "itemSet");

		StringBuilder builder = new StringBuilder();
		for (List<Integer> integers : transactionList.collect()) {
			String listString = integers.toString();
			builder.append(listString.substring(1, listString.length()-1));
			builder.append("\n");
		}
		try{
			PrintWriter writer = new PrintWriter("the-file-name.csv", "UTF-8");
			writer.println(builder.toString());
			writer.close();
		} catch (IOException e) {
		}


		System.out.println("\n****** Number of transactions:" + transactionList.count());
		/**
		 * MINING PART
		 */

		/*
		 * send transactions to Apriori with .mine(DataSet<List<Integer>,
		 * MinimumSupport, Iterations) no changes to BasketAnalysis
		 */
		 /*
		 * send transactions to Apriori with .mine(DataSet<List<Integer>,
		 * MinimumSupport, Iterations) no changes to BasketAnalysis
		 */

		DataSet<ItemSet> items = Apriori.mine(transactionList, minSupport, numIterations);

		/**
		 * POST PROCESSING
		 */

		/*
		 * For the final result the last transformation is the map back of the
		 * StockCodes no changes to BasketAnalysis
		 */

		DataSet<List<String>> transactionListMapped = items.flatMap(
				new MapBackTransactions1()).withBroadcastSet(itemsSet,
				"itemSet");

		transactionListMapped.print();

	} // end main method

	public static final class AssignUniqueId
			implements
			MapFunction<Tuple3<String, String, String>, Tuple2<Integer, String>> {

		private static final long serialVersionUID = 1L;
		int numItems = 1;

		@Override
		public Tuple2<Integer, String> map(Tuple3<String, String, String> value) {

			return new Tuple2<>(numItems++, value.f1);
		}
	}

	public static final class AssignUniqueId2
			implements
			MapFunction<Tuple2<String, String>, Tuple2<Integer, String>> {

		private static final long serialVersionUID = 1L;
		int numItems = 0;

		@Override
		public Tuple2<Integer, String> map(Tuple2<String, String> value) {

			return new Tuple2<>(numItems++, value.f1);
		}
	}

	public static class GroupItemsInListPerBill
			extends
			RichGroupReduceFunction<Tuple3<String, String, String>, List<Integer>> {

		private static final long serialVersionUID = 1L;
		// get the set of items or products and create a hash map,
		// so to map product codes into integers
		private Collection<Tuple2<Integer, String>> itemSet;
		// these is the set of possible items or products
		private Hashtable<String, Integer> itemSetMap = new Hashtable<String, Integer>();
		;

		@Override
		public void open(Configuration parameters) throws Exception {
			this.itemSet = getRuntimeContext().getBroadcastVariable("itemSet");
			// Tuple2<map integer, product code>
			for (Tuple2<Integer, String> item : itemSet) {
				// itemSetMap.put(key: product code, value: map integer);
				itemSetMap.put(item.f1, item.f0);
			}

		}

		@Override
		public void reduce(
				Iterable<Tuple3<String, String, String>> transactions,
				Collector<List<Integer>> out) {

			// transactions contains all the transactions with the same
			// InvoiceNo
			// collect here the products of all those transactions and map them
			// into integers
			// also keep the timestamp(InvoiceDate)to sort them afterwards
			List<Tuple2<Integer, String>> list_items = new ArrayList<Tuple2<Integer, String>>();

			// f0 f1 f2
			// Tuple3<transaction number, product, timestamp>
			for (Tuple3<String, String, String> trans : transactions) {
				if (itemSetMap.containsKey(trans.f1))
					list_items.add(new Tuple2<Integer, String>(itemSetMap
							.get(trans.f1), trans.f2));
				// System.out.println("   " + trans.f0 + " " + trans.f2 + " " +
				// itemSetMap.get(trans.f2));
			}

			// After the mapping process we create the final list of integer,
			// with the same size as list_items new
			// ArrayList<Integer>(list_items.size());
			List<Integer> items = new ArrayList<Integer>(list_items.size());
			// now we can sort the list by the time assuming
			// that one InvoiceNo has the same date
			list_items.sort(new Comparator<Tuple2<Integer, String>>() {

				@Override
				public int compare(Tuple2<Integer, String> o1,
								   Tuple2<Integer, String> o2) {
					// the time column looks like this: 12/01/2010 08:26:00
					// as we only need the time we can use a substring starting
					// at index 11
					Integer comparison = 0;
					if (o1.f1.length() > 10 && o2.f1.length() > 10) {
						// we split the original String and only consider the
						// time
						String time1 = o1.f1.substring(11);
						String time2 = o2.f1.substring(11);

						try {
							// we finally define a Format for the time and parse
							// the Substring
							DateFormat dateFormat = new SimpleDateFormat(
									"HH:mm:ss");
							Date timeO1 = dateFormat.parse(time1);
							Date timeO2 = dateFormat.parse(time2);
							comparison = timeO1.compareTo(timeO2);
						} catch (ParseException e) {
							e.printStackTrace();
						}
					}
					return comparison;

				}

			});

			// In the last step of this method we add the sorted list elements
			// on after another into our wanted result format
			for (int i = 0; i < list_items.size() - 1; i++) {
				items.add(list_items.get(i).f0);
			}
			if (!items.isEmpty())
				out.collect(items);
		}
	}

	public static class GroupItemsInListPerBill2
			extends
			RichGroupReduceFunction<Tuple2<String, String>, List<Integer>> {

		private static final long serialVersionUID = 1L;
		// get the set of items or products and create a hash map,
		// so to map product codes into integers
		private Collection<Tuple2<Integer, String>> itemSet;
		// these is the set of possible items or products
		private Hashtable<String, Integer> itemSetMap = new Hashtable<String, Integer>();
		;

		@Override
		public void open(Configuration parameters) throws Exception {
			this.itemSet = getRuntimeContext().getBroadcastVariable("itemSet");
			// Tuple2<map integer, product code>
			for (Tuple2<Integer, String> item : itemSet) {
				// itemSetMap.put(key: product code, value: map integer);
				itemSetMap.put(item.f1, item.f0);
			}


		}

		@Override
		public void reduce(
				Iterable<Tuple2<String, String>> transactions,
				Collector<List<Integer>> out) {

			// transactions contains all the transactions with the same
			// InvoiceNo
			// collect here the products of all those transactions and map them
			// into integers
			// also keep the timestamp(InvoiceDate)to sort them afterwards
			List<Tuple2<Integer, String>> list_items = new ArrayList<Tuple2<Integer, String>>();

			// f0 f1 f2
			// Tuple3<transaction number, product, timestamp>
			for (Tuple2<String, String> trans : transactions) {
				if (itemSetMap.containsKey(trans.f1))
					list_items.add(new Tuple2<Integer, String>(itemSetMap
							.get(trans.f1), trans.f0));
				System.out.println("Look at here " + trans.f0 + " " + trans.f1 + " ");
			}

			// After the mapping process we create the final list of integer,
			// with the same size as list_item
			// In the last step of this methods new
			// ArrayList<Integer>(list_items.size());
			List<Integer> items = new ArrayList<Integer>(list_items.size());
			// now we can sort the list by the time assuming
			// that one InvoiceNo has the same date
			//we add the sorted list elements
			// on after another into our wanted result format
			for (int i = 0; i < list_items.size() - 1; i++) {
				items.add(list_items.get(i).f0);
			}
			if (!items.isEmpty())

				out.collect(items);
		}
	}

	public static class MapBackTransactions1 extends
			RichFlatMapFunction<ItemSet, List<String>> {

		private static final long serialVersionUID = 1L;
		// get the set of items or products and create a hash map, so to map
		// product codes into integers
		private Collection<Tuple2<Integer, String>> itemSet; // these is the set
		// of possible
		// items or
		// products
		// here create the Hash map the other way around to map back
		private Hashtable<Integer, String> itemSetMap = new Hashtable<Integer, String>();
		;

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

	public static final class ReadInFilter implements
			FilterFunction<Tuple3<String, String, String>> {

		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(Tuple3<String, String, String> value) {
			if (value.f0 != null && value.f0 != "")
				if (value.f1 != null && value.f1 != "")
					if (value.f2 != null && value.f2 != "")
						return true;
			return false;
		}
	}

	public static final class ReadInFilter2 implements FilterFunction<Tuple2<String, String>> {
		@Override
		public boolean filter(Tuple2<String, String> value) {
			if (value.f0 != null && value.f0 != "")
				if (value.f1 != null && value.f1 != "")
					return true;
			return false;
		}

	}
}