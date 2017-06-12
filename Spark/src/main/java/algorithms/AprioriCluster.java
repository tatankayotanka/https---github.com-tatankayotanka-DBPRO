package algorithms;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class AprioriCluster {
	// final version
	static int iterator = 2;
	static Map<Integer, JavaPairRDD<List<Integer>, Integer>> resultglobal = new LinkedHashMap<>();
	private static final Pattern SPACE = Pattern.compile(" ");
	static long time;
	

	public static void main(String[] arg0) {
		JavaSparkContext ctx = new JavaSparkContext(new SparkConf());
		ctx.setLogLevel("WARN");
		time = System.currentTimeMillis();
		/**
		 * input: args[0] path of the dataset to analyse 
		 * args[1] the min_support value (positve Integer value) 
		 * args[2] the nummer of iterations (positiv Integer value) 
		 * args[3] Y if you want to search for larger
		 * k-itemsets, N if you just interested in args[2]-itemsets
		 * 
		 * read in every line of the selected textfile
		 */
		JavaRDD<String> lines = ctx.textFile(arg0[0], 1);
		/**
		 * set the wanted min_sup
		 */
		final Broadcast<Integer> support = ctx.broadcast(Integer
				.parseInt(arg0[1]));
		int iteration = Integer.parseInt(arg0[2]);
		String goahead = arg0[3].toUpperCase();
		/**
		 * get all productIDs with support over min_sup from dataset
		 */
		final JavaPairRDD<Integer, Integer> words = lines
				.flatMapToPair(
						new PairFlatMapFunction<String, Integer, Integer>() {
							private static final long serialVersionUID = 1L;

							@Override
							public Iterable<Tuple2<Integer, Integer>> call(
									String s) {
								ArrayList<Tuple2<Integer, Integer>> call = new ArrayList<>();
								for (String a : (Arrays.asList(SPACE.split(s)))) {
									int i = Integer.parseInt(a);
									call.add(new Tuple2<Integer, Integer>(i, 1));
								}
								return call;
							}
						}).reduceByKey(Func)
				.filter(new Function<Tuple2<Integer, Integer>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<Integer, Integer> i)
							throws Exception {
						if (i._2 >= support.value())
							return true;
						return false;
					}
				});

		final Broadcast<List<Integer>> product = ctx.broadcast(words.keys()
				.collect());

		/**
		 * transform lines into an ArrayList including all pId's that fulfill
		 * min_sup otherwise pID will be removed from transaction also removes
		 * duplicates in transactions
		 */
		JavaRDD<ArrayList<Integer>> transaction = lines
				.map(new Function<String, ArrayList<Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public ArrayList<Integer> call(String s) throws Exception {
						String[] parts = s.split(" ");
						ArrayList<Integer> t = new ArrayList<Integer>();

						for (int i = 0; i <= parts.length - 1; i++) {
							int j = Integer.parseInt(parts[i]);
							if (!t.contains(j) && product.value().contains(j)) {
								t.add(j);
							}
						}
						return (t);
					}
				}).filter(filterNULL2).cache();

		/**
		 * 2. iteration from R-Apriori is used here to reduce candidate
		 * generation for the 2-itemset generation
		 */
		JavaPairRDD<List<Integer>, Integer> it = transaction.flatMapToPair(new PairFlatMapFunction<ArrayList<Integer>, List<Integer>, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<List<Integer>, Integer>> call(
					ArrayList<Integer> s) throws Exception {
				List<Tuple2<List<Integer>, Integer>> candidate = new ArrayList<Tuple2<List<Integer>, Integer>>();

				for (Integer g : product.value()) {
					if (s.contains(g)) {
						for (Integer k : s) {
							int i = g.compareTo(k);
							if (i < 0) {
								List<Integer> candi = new ArrayList<Integer>(2);
								candi.add(0, g);
								candi.add(1, k);
								candidate.add(new Tuple2<List<Integer>, Integer>(
										candi, 1));
							}
						}
					}
				}
				return candidate;
			}
		}).reduceByKey(Func)
				.filter(new Function<Tuple2<List<Integer>, Integer>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<List<Integer>, Integer> i) throws Exception {
						if (i != null && i._2 >= support.value())
							return true;
						return false;
					}
				}).cache();
		
		resultglobal.put(iterator, it);
		iterator++;
		//final Broadcast<List<ArrayList<Integer>>> trans = ctx.broadcast(transaction.collect());
		List<ArrayList<Integer>> trans = new ArrayList<ArrayList<Integer>>();	 
		//iterations
		for (int i = iteration - 3; i >= 0; i--) {
			JavaPairRDD<List<Integer>, List<Integer>> it1 = it.keys().cartesian(it.keys());
			/**
			 * candidate generation
			 */
			JavaRDD<List<Integer>> keySet = it1
					.map(new Function<Tuple2<List<Integer>, List<Integer>>, List<Integer>>() {
						private static final long serialVersionUID = 1L;

						@Override
						public List<Integer> call(
								Tuple2<List<Integer>, List<Integer>> arg0)
								throws Exception {
							int size = arg0._1.size()+1;
							if (arg0._1.get(0) <= arg0._2.get(0)
									&& arg0._1.get(size - 2) >= arg0._2
											.get(0)) {
								List<Integer> key = new ArrayList<Integer>();
								key.addAll(0, arg0._1());
								int counter = 0;
								for (int i = 0; i < arg0._2.size(); i++) {
									if (!key.contains(arg0._2.get(i))) {
										counter++;
										if (counter >= 2)
											return null;
										else
											key.add(arg0._2.get(i));
									}
								}
								if (counter == 1) {
									Collections.sort(key);
									return key;
								}
							}
							return null;
						}
					}).filter(filterNULL3).distinct().cache();	
			
			if(iterator == 3){
				
			final List<Integer> productNew = keySet
					.flatMap(new FlatMapFunction<List<Integer>, Integer>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Iterable<Integer> call(List<Integer> arg0)
								throws Exception {
							return arg0;
						}

					}).distinct().collect();
			final Broadcast<List<Integer>> prodNew = ctx.broadcast(productNew);
				/**
				 * filter removes all transaction including less items than the
				 * current k-itemset requires
				 */
				trans = transaction.filter(
						new Function<ArrayList<Integer>, Boolean>() {
							private static final long serialVersionUID = 1L;

							@Override
							public Boolean call(ArrayList<Integer> arg0)
									throws Exception {
								arg0.retainAll(prodNew.value());
								if (arg0.size() >= 3)
									return true;
								return false;
							}
						}).collect();
				
			
			}
			final Broadcast<List<ArrayList<Integer>>> transn = ctx.broadcast(trans);
			it = keySet
					.flatMapToPair(new PairFlatMapFunction<List<Integer>, List<Integer>, Integer>() {
						private static final long serialVersionUID = 1L;
						
						@Override
						public Iterable<Tuple2<List<Integer>, Integer>> call(
								List<Integer> s) throws Exception {

							List<Tuple2<List<Integer>, Integer>> candidate = new ArrayList<Tuple2<List<Integer>, Integer>>();
							for (ArrayList<Integer> g : transn.value()) {
								if (g.containsAll(s)) {
									candidate.add(new Tuple2<List<Integer>, Integer>(s, 1));
								}
							}
							return candidate;
						}
					}).reduceByKey(Func).filter(new Function<Tuple2<List<Integer>, Integer>, Boolean>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Boolean call(Tuple2<List<Integer>, Integer> i) throws Exception {
							if (i != null && i._2 >= support.value())
								return true;
							return false;
						}
					}).cache();
			
			if (i == 0) {
				int k = it.take(2).size();
				if (k <= 1) {
					if (k == 0) {
						result(iterator - 1);
					} else {
						resultglobal.put(iterator, it);
						result(iterator);
					}
				} else {
					if (goahead.contains("N")) {
						resultglobal.put(iterator, it);
						result(iterator);
					} else {
						resultglobal.put(iterator, it);
						iterator++;
						i = i + 1;
					}
				}
			} else {
				resultglobal.put(iterator, it);
				iterator++;
			}
		}
		ctx.stop();
		ctx.close();
		System.out.println("****  Apriori takes: " + time / 1000 + "s. ****");
	}

	private static Map<Integer, JavaPairRDD<List<Integer>, Integer>> result(
			int it) {
		List<List<Integer>> itemset = resultglobal.get(it).keys().collect();
		time = System.currentTimeMillis() - time;
		if (itemset.isEmpty())
			System.out.println("No FIS found, please try with less iterations");
		else {
			for (List<Integer> i : itemset)
				System.out.println(i);
		}
		return resultglobal;
	}

	// Functions

	static Function2<Integer, Integer, Integer> Func = new Function2<Integer, Integer, Integer>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Integer call(Integer arg0, Integer arg1) throws Exception {
			int i = arg0 + arg1;
			return i;
		}
	};
	
	static Function<List<Integer>, Boolean> filterNULL3 = new Function<List<Integer>, Boolean>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Boolean call(List<Integer> arg0) throws Exception {
			if (arg0 != null && !arg0.isEmpty())
				return true;
			return false;
		}
	};

	static Function<ArrayList<Integer>, Boolean> filterNULL2 = new Function<ArrayList<Integer>, Boolean>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Boolean call(ArrayList<Integer> arg0) throws Exception {
			if (arg0 != null && arg0.size() > 1)
				return true;
			return false;
		}

	};	

}