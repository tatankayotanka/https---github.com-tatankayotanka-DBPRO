package algorithms;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import java.util.*;

/**
 * Ariane
 */

public class ECLAT {

	/**
	 * Pojo for itemsets, where itemset includes all frequent items of the set
	 * and bits the tids
	 */
	public static class ItemSet {

		public List<Integer> itemset = new ArrayList<Integer>();
		public BitSet bits = new BitSet();

		public ItemSet(List<Integer> args, BitSet arg0) {
			this.itemset = args;
			this.bits.or(arg0);
		}

		public String getId() {
			return this.itemset.toString();
		}

		@Override
		public String toString() {
			return this.itemset.toString();
		}

		
	}

	private static int num_transaction = -1;

	public static void main(String[] args) throws Exception {
//		 String dataPath = "./src/main/resources/T10I4D100K.dat";
//		 final int minSupport = 500;
//		 final int iterations = 5;
		String dataPath = args[0];
		final int minSupport = Integer.parseInt(args[1]);
		final int iterations = Integer.parseInt(args[2]);
		long startTime = System.currentTimeMillis();

		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		/**
		 * get input data, splits per line and adds tid
		 */
		DataSet<String> text = env.readTextFile(dataPath);
		DataSet<Tuple2<Integer, List<Integer>>> transaction = text
				.flatMap(new FlatMapFunction<String, Tuple2<Integer, List<Integer>>>() {
					private static final long serialVersionUID = 1L;

					//private int num_transaction = -1;
					@Override
					public void flatMap(String in,
							Collector<Tuple2<Integer, List<Integer>>> out)
							throws Exception {
						List<Integer> transactionList = new ArrayList<Integer>();
						for (String pid : Arrays.asList(in.split(" "))) {
							// TODO split is fix here
							Integer pidInt = Integer.parseInt(pid);
							if (!transactionList.contains(pidInt))
								transactionList.add(pidInt);
						}
						
						if(transactionList.size()>iterations){
						num_transaction++;						
						out.collect(new Tuple2<Integer, List<Integer>>(
								num_transaction, transactionList));
						}
					}
				});
		
		//transaction.print();

		/**
		 * gets all frequent singletons from the dataset (Apriori method)
		 */
		DataSet<Integer> itemCounts = transaction
				.flatMap(new TransactionSplitter()).groupBy(0).sum(1)
				.filter(new ItemSetFilterTuple2(minSupport))
				.map(new FieldSelector());

		if (iterations > 1) {

			/**
			 * CHANGE TO ECLAT this method gets all singletons from previous
			 * DataSet "itemCounts" and, using the DataSet "transaction" as
			 * Broadacast, creates the tidlist (tid) for the ECLAT for each
			 * frequent item (pid) it1 = iteration 1
			 */
			DataSet<Tuple2<Integer, BitSet>> it1 = itemCounts
					.flatMap(
							new RichFlatMapFunction<Integer, Tuple2<Integer, BitSet>>() {
								private static final long serialVersionUID = 1L;
								private Collection<Tuple2<Integer, List<Integer>>> transaction;

								@Override
								public void open(Configuration parameters)
										throws Exception {
									this.transaction = getRuntimeContext()
											.getBroadcastVariable("transaction");
								}

								@Override
								public void flatMap(Integer pid,
										Collector<Tuple2<Integer, BitSet>> out)
										throws Exception {
									BitSet tid = new BitSet();
									for (Tuple2<Integer, List<Integer>> list : transaction) {
										if (list.f1.indexOf(pid) != -1) {
											tid.set(list.f0);
										}
									}
									out.collect(new Tuple2<Integer, BitSet>(
											pid, tid));
								}
							}).withBroadcastSet(transaction, "transaction");
					//.filter(new ItemSetFilterInt(minSupport));

			DataSet<Tuple3<Integer, Integer, BitSet>> it2 = it1
					.combineGroup(new CandidateGeneration2(minSupport));
			//it2.print();

			if (iterations > 2) {

				DataSet<ItemSet> it3 = it2						
						.combineGroup(new CandidateGeneration3(minSupport))
						.distinct(new ItemSetKey());
						//.distinct(0, 1, 2);
//
//				if (iterations > 3) {
//
//					DataSet<ItemSet> it4 = it3							
//							.combineGroup(
//							new CandidateGeneration4(minSupport))
//							.distinct(new ItemSetKey());
//							//new ItemSetKey4());

					if (iterations - 3 >= 1) {
						IterativeDataSet<ItemSet> prevSet = it3
								.iterate(iterations - 3);

						/**
						 * iterative creates the next bigger itemset
						 */
						DataSet<ItemSet> itn = prevSet.combineGroup(
								new CandidateGeneration(minSupport)).distinct(
								new ItemSetKey());

						prevSet.closeWith(itn).print();

//					} else
//						it4.print();

				} else
					it3.print();
			} else
				it2.map(new FieldSelector3()).print();
		} else
			itemCounts.print();

		long estimatedTime = System.currentTimeMillis() - startTime;
		System.out.println("Estimated time (sec): " + estimatedTime / 1000.0);

	}// main

	public static final class ItemSetFilterInt implements
			FilterFunction<Tuple2<Integer, BitSet>> {
		private static final long serialVersionUID = 1L;
		private Integer minSupport;

		public ItemSetFilterInt(int minSupport) {
			super();
			this.minSupport = minSupport;
		}

		@Override
		public boolean filter(Tuple2<Integer, BitSet> value) {
			return value.f1.cardinality() >= minSupport;
		}
	}

	public static final class CandidateGeneration implements
			GroupCombineFunction<ItemSet, ItemSet> {

		private static final long serialVersionUID = 1L;
		private Integer minSupport;

		public CandidateGeneration(int minSupport) {
			super();
			this.minSupport = minSupport;
		}

		@Override
		public void combine(Iterable<ItemSet> arg0, Collector<ItemSet> arg1)
				throws Exception {
			List<ItemSet> l1 = new ArrayList<ItemSet>();
			for (ItemSet i : arg0) {
				l1.add(i);
			}
			for (int i = 0; i <= l1.size() - 1; i++) {
				for (int j = i + 1; j <= l1.size() - 1; j++) {
					List<Integer> key = new ArrayList<Integer>();

					key.addAll(l1.get(i).itemset);
					int counter = 0;
					for (Integer b : l1.get(j).itemset) {

						if (!key.contains(b)) {
							counter++;
							if (counter >= 2)
								break;
							else
								key.add(b);
						}
					}

					if (counter == 1) {

						BitSet tid = (BitSet) l1.get(i).bits.clone();
						(tid).and(l1.get(j).bits);

						if (tid.cardinality() >= minSupport) {
							Collections.sort(key);
							arg1.collect(new ItemSet(key, tid));
						}
					}
				}// out if
			}
		}// out for
	}

	public static class TransactionSplitter
			implements
			FlatMapFunction<Tuple2<Integer, List<Integer>>, Tuple2<Integer, Integer>> {
		/**
		 * returns each item of one transaction with a count of 1
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple2<Integer, List<Integer>> transaction,
				Collector<Tuple2<Integer, Integer>> out) {
			for (Integer item : transaction.f1) {
				out.collect(new Tuple2<Integer, Integer>(item, 1));
			}
		}
	}

	public static final class ItemSetFilterTuple2 implements
			FilterFunction<Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;
		private Integer minSupport;

		public ItemSetFilterTuple2(int minSupport) {
			super();
			this.minSupport = minSupport;
		}

		@Override
		public boolean filter(Tuple2<Integer, Integer> value) {
			return value.f1 >= minSupport;

		}
	}

	public static final class FieldSelector implements
			MapFunction<Tuple2<Integer, Integer>, Integer> {

		private static final long serialVersionUID = 1L;

		@Override
		public Integer map(Tuple2<Integer, Integer> value) {
			return value.f0;
		}
	}

	public static final class FieldSelector4
			implements
			MapFunction<Tuple4<Integer, Integer, Integer, BitSet>, Tuple4<Integer, Integer, Integer, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple4<Integer, Integer, Integer, Integer> map(
				Tuple4<Integer, Integer, Integer, BitSet> value) {
			return new Tuple4<Integer, Integer, Integer, Integer>(value.f0,
					value.f1, value.f2, value.f3.cardinality());
		}
	}

	public static final class FieldSelector3
			implements
			MapFunction<Tuple3<Integer, Integer, BitSet>, Tuple3<Integer, Integer, Integer>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple3<Integer, Integer, Integer> map(
				Tuple3<Integer, Integer, BitSet> value) {
			return new Tuple3<Integer, Integer, Integer>(value.f0, value.f1,
					value.f2.cardinality());
		}
	}

	public static class ItemSetKey implements KeySelector<ItemSet, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(ItemSet i) throws Exception {

			return i.getId();
		}

	}

	public static final class CandidateGeneration2
			implements
			GroupCombineFunction<Tuple2<Integer, BitSet>, Tuple3<Integer, Integer, BitSet>> {
		private static final long serialVersionUID = 1L;
		private Integer minSupport;

		public CandidateGeneration2(int minSupport) {
			super();
			this.minSupport = minSupport;
		}

		@Override
		public void combine(Iterable<Tuple2<Integer, BitSet>> arg0,
				Collector<Tuple3<Integer, Integer, BitSet>> out)
				throws Exception {

			List<Tuple2<Integer, BitSet>> l1 = new ArrayList<Tuple2<Integer, BitSet>>();
			for (Tuple2<Integer, BitSet> i : arg0) {
				l1.add(i);
			}

			for (int i = 0; i <= l1.size() - 1; i++) {
				for (int j = i + 1; j <= l1.size() - 1; j++) {
					int k1 = l1.get(i).f0;
					int k2 = l1.get(j).f0;
					BitSet tid = (BitSet) l1.get(i).f1.clone();
					(tid).and(l1.get(j).f1);

					if (tid.cardinality() >= minSupport) {
						if (k1 < k2)
							out.collect(new Tuple3<Integer, Integer, BitSet>(
									k1, k2, tid));
						if (k1 > k2)
							out.collect(new Tuple3<Integer, Integer, BitSet>(
									k2, k1, tid));
					}
				}
			}
		}// out for
	}

	public static final class CandidateGeneration3
			implements
			GroupCombineFunction<Tuple3<Integer, Integer, BitSet>, ItemSet> {

		private static final long serialVersionUID = 1L;
		private Integer minSupport;

		public CandidateGeneration3(int minSupport) {
			super();
			this.minSupport = minSupport;
			//System.out.println(this.minSupport);
		}

		@Override
		public void combine(Iterable<Tuple3<Integer, Integer, BitSet>> arg0,
				Collector<ItemSet> out)
				throws Exception {

			List<Tuple3<Integer, Integer, BitSet>> l1 = new ArrayList<Tuple3<Integer, Integer, BitSet>>();
			for (Tuple3<Integer, Integer, BitSet> i : arg0) {
				l1.add(i);
			}

			for (int i = 0; i <= l1.size() - 1; i++) {
				for (int j = i + 1; j <= l1.size() - 1; j++) {

//					if (l1.get(i).f0 == l1.get(j).f0
//							|| l1.get(i).f1 == l1.get(j).f1
//							|| l1.get(i).f1 == l1.get(j).f0
//							|| l1.get(i).f0 == l1.get(j).f1) {

						List<Integer> key = new ArrayList<Integer>();
						key.add(0, l1.get(i).f0);
						key.add(1, l1.get(i).f1);
						int counter = 0; 
						if (!key.contains(l1.get(j).f0)) {
							key.add(2, l1.get(j).f0);
							counter++;
						} else {
							if (!key.contains(l1.get(j).f1)) {
								key.add(2, l1.get(j).f1);
								counter++;
							}
						}
						if (counter == 1) {
							BitSet tid = (BitSet) l1.get(i).f2.clone();
							(tid).and(l1.get(j).f2);
							if (tid.cardinality() >= minSupport) {
								Collections.sort(key);
								out.collect(new ItemSet(
										key, tid));
							}
						}
					//}// if
				}
			}
		}// combine
	}

//	public static final class ItemSetKey4 implements
//			KeySelector<ItemSet, Tuple4<Integer, Integer, Integer, Integer>> {
//		private static final long serialVersionUID = 1L;
//
//		@Override
//		public Tuple4<Integer, Integer, Integer, Integer> getKey(ItemSet i)
//				throws Exception {
//			Collections.sort(i.itemset);
//			return i.getTuple4();
//		}

//	}

	public static final class CandidateGeneration4
			implements
			GroupCombineFunction<Tuple4<Integer, Integer, Integer, BitSet>, ItemSet> {

		private static final long serialVersionUID = 1L;
		private Integer minSupport;

		public CandidateGeneration4(int minSupport) {
			super();
			this.minSupport = minSupport;
		}

		@Override
		public void combine(
				Iterable<Tuple4<Integer, Integer, Integer, BitSet>> arg0,
				Collector<ItemSet> out) throws Exception {

			List<Tuple4<Integer, Integer, Integer, BitSet>> l1 = new ArrayList<Tuple4<Integer, Integer, Integer, BitSet>>();
			for (Tuple4<Integer, Integer, Integer, BitSet> i : arg0) {
				l1.add(i);
			}
			for (int i = 0; i <= l1.size() - 1; i++) {
				for (int j = i + 1; j <= l1.size() - 1; j++) {
					List<Integer> key = new ArrayList<Integer>();

					key.add(l1.get(i).f0);
					key.add(l1.get(i).f1);
					key.add(l1.get(i).f2);

					int counter = 0;
					if (!key.contains(l1.get(j).f0)) {
						key.add(l1.get(j).f0);
						counter++;
					}
					if (!key.contains(l1.get(j).f1)) {
						key.add(l1.get(j).f1);
						counter++;
					}
					if (!key.contains(l1.get(j).f2)) {
						key.add(l1.get(j).f2);
						counter++;
					}

					if (counter == 1) {

						BitSet tid = (BitSet) l1.get(i).f3.clone();
						(tid).and(l1.get(j).f3);

						if (tid.cardinality() >= minSupport) {
							Collections.sort(key);
							out.collect(new ItemSet(key, tid));
						}
					}
				}// out if
			}
		}// out for
	}
}