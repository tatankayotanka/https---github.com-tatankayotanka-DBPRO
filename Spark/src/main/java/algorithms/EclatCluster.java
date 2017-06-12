package algorithms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

//final version 
public class EclatCluster {

	/**
	 * Map including results of iterations
	 */
	static Map<Integer, JavaPairRDD<List<Integer>, BitSet>> resultglobal = new LinkedHashMap<>();
	private static final Pattern SPACE = Pattern.compile(" ");
	/**
	 * counter for tids
	 */
	static int counter = -1;
	static long time;
	static int iterator = 2;

	public static void main(String[] args) {
		JavaSparkContext ctx = new JavaSparkContext(new SparkConf());
		ctx.setLogLevel("WARN");
		
		time = System.currentTimeMillis();
		/**
		 * input: 
		 * args[0] path of the dataset to analyse
		 * args[1] the min_support value (positve Integer value)
		 * args[2] the nummer of iterations  (positiv Integer value)
		 * args[3] Y if you want to search for larger k-itemsets, N if you just interested in args[2]-itemsets
		 * args[4] numOfPartitions
		 * 
		 * read in every line of the selected textfile
		 */
		JavaRDD<String> lines = ctx.textFile(args[0], Integer.parseInt(args[4]));
		/**
		 * set the wanted min_sup and broadcast it
		 */		
		final Broadcast<Integer> support = ctx.broadcast(Integer
				.parseInt(args[1]));
		int iteration = Integer.parseInt(args[2]);
		String goahead = args[3].toUpperCase();

		/**
		 * add tid to each transaction (one line from textfile)
		 */
		JavaPairRDD<Integer, String> transaction = lines
				.keyBy(new Function<String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(String s) {
						counter++;
						return counter;
					}
				});
		/**
		 * following two Functions get all productIDs with a BitSet from the
		 * dataset
		 */
		JavaPairRDD<List<Integer>, BitSet> pId = transaction
				.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, String>, List<Integer>, BitSet>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<List<Integer>, BitSet>> call(
							Tuple2<Integer, String> arg0) throws Exception {

						ArrayList<Tuple2<List<Integer>, BitSet>> call = new ArrayList<>();
						for (String a : (Arrays.asList(SPACE.split(arg0._2)))) {
							BitSet t = new BitSet(counter);
							List<Integer> b = new ArrayList<Integer>();
							int c = Integer.parseInt(a);
							b.add(c);
							t.set(arg0._1);
							call.add(new Tuple2<List<Integer>, BitSet>(b, t));
						}
						return call;
					}
				});

		JavaPairRDD<List<Integer>, BitSet> it1 = pId.reduceByKey(createTID)
				.filter(new Function<Tuple2<List<Integer>, BitSet>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<List<Integer>, BitSet> arg0)
							throws Exception {
						if (arg0 != null
								&& arg0._2.cardinality() >= support.value())
							return true;
						return false;
					}
				}).persist(StorageLevel.MEMORY_AND_DISK());
		
		for (int i = iteration - 2; i >= 0; i--) {
			JavaPairRDD<Tuple2<List<Integer>, BitSet>, Tuple2<List<Integer>, BitSet>> it = it1
					.cartesian(it1).persist(StorageLevel.MEMORY_AND_DISK());

			it1 = it.mapToPair(
					new PairFunction<Tuple2<Tuple2<List<Integer>, BitSet>, Tuple2<List<Integer>, BitSet>>, List<Integer>, BitSet>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<List<Integer>, BitSet> call(
								Tuple2<Tuple2<List<Integer>, BitSet>, Tuple2<List<Integer>, BitSet>> arg0)
								throws Exception {
							List<Integer> key = new ArrayList<Integer>();
							int size = arg0._1._1.size() + 1;
							if (arg0._1._1.get(0) <= arg0._2._1.get(0)) {
								if (size == 2
										|| (size > 2 && arg0._1._1
												.get(size - 2) >= arg0._2._1
												.get(0))) {
									key.addAll(0, arg0._1._1());
									int counter = 0;
									for (int i = 0; i < arg0._2._1.size(); i++) {

										if (!key.contains(arg0._2._1.get(i))) {
											counter++;
											if (counter >= 2)
												return null;
											else
												key.add(arg0._2._1.get(i));
										}
									}
									if (counter == 1) {
										Collections.sort(key);
										BitSet tid = (BitSet) arg0._1._2
												.clone();
										(tid).and(arg0._2._2);
										if (tid.cardinality() >= support
												.value())
											return new Tuple2<List<Integer>, BitSet>(
													key, tid);
										else
											return null;
									}
								}
							}
							return null;
						}
					}).filter(new Function<Tuple2<List<Integer>, BitSet>, Boolean>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Boolean call(Tuple2<List<Integer>, BitSet> arg0)
								throws Exception {
							if (arg0 != null)
								return true;
							return false;
						}
					}).reduceByKey(new Function2<BitSet, BitSet, BitSet>(){

						@Override
						public BitSet call(BitSet v1, BitSet v2)
								throws Exception {
							v1.or(v2);
							return v1;
						}						
					}).persist(StorageLevel.MEMORY_AND_DISK());
			
			if (i == 0) {
				int k = it1.take(2).size();
				if (k <= 1) {
					if (k == 0) {
						result(iterator - 1);
					} else {
						resultglobal.put(iterator, it1);
						result(iterator);
					}
				} else {
					if (goahead.contains("N")) {
						resultglobal.put(iterator, it1);
						result(iterator);
					} else {
						resultglobal.put(iterator, it1);
						iterator++;
						i = i + 1;
					}
				}
			} else {
				resultglobal.put(iterator, it1);
				iterator++;
			}
		}
		ctx.stop();
		ctx.close();
		System.out.println("****  Eclat takes: " + time / 1000 + "s. ****");
	}

	/**
	 * method returns the result of the calculation
	 */
	private static Map<Integer, JavaPairRDD<List<Integer>, BitSet>> result(
			int it) {

		List<List<Integer>> itemset = resultglobal.get(it).keys().collect();
		time = System.currentTimeMillis() - time;
		if (itemset.isEmpty())
			System.out.println("No FIS found, please try with less iterations");
		else{
			for(List<Integer> i : itemset)
				System.out.println(i);
		}
		return resultglobal;
	}

	/**
	 * used functions 1. : used for the reducebyKey() in the initial part
	 */
	static Function2<BitSet, BitSet, BitSet> createTID = new Function2<BitSet, BitSet, BitSet>() {

		private static final long serialVersionUID = 1L;

		@Override
		public BitSet call(BitSet arg0, BitSet arg1) throws Exception {
			arg0.or(arg1);
			return arg0;
		}
	};

}
