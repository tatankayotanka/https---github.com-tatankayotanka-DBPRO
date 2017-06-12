package algorithms;


import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;



public class FPGrowthExample {
	
	public static void main(String[] args) throws Exception {
		
		// Create a Java Spark Context		
	   
	    JavaSparkContext sc = new JavaSparkContext(new SparkConf());
	    		//"local", "Simple FPGrowthExample",
	    	    // "/Downloads/spark-1.3.1-bin-hadoop2.6/spark-1.3.1-bin-hadoop2.6/", new String[]{"target/simple-project-1.0.jar"});      
	    sc.setLogLevel("WARN");	
		long time3 = System.currentTimeMillis();
	    
		// alle Zeilen aus testdata
		//JavaRDD<String> data = sc.textFile("src/main/resources/SyntheticData-T10I6D1e+05.txt");
		JavaRDD<String> data = sc.textFile(args[0],Integer.parseInt(args[2]));

		JavaRDD<List<String>> transactions = data.map(
		  new Function<String, List<String>>() {
		    private static final long serialVersionUID = 1L;

			public List<String> call(String line) {
		      String[] parts = line.split(" ");
		      List<String> parts2 = new ArrayList<String>();
		      for(int i = 0; i<= parts.length-1; i++){
		    	  if(!parts2.contains(parts[i])) parts2.add(parts[i]);
		      }
		      return parts2;
		    }
		  }
		);
		
	       
		FPGrowth fpg = new FPGrowth()
		  .setMinSupport( Double.parseDouble(args[1]))
		 // .setMinSupport( 0.003)
		  .setNumPartitions(Integer.parseInt(args[2]));
		
		FPGrowthModel<String> model = fpg.run(transactions);

		List<FPGrowth.FreqItemset<String>> i = model.freqItemsets().toJavaRDD().collect();
//		for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
//		
//		 // System.out.println("[" + itemset.javaItems()+",");
//		}
	

//		double minConfidence = 0.0;
//		for (AssociationRules.Rule<String> rule
//		  : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
//		  System.out.println(
//		    rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
//		}
		time3 = System.currentTimeMillis() - time3;
		sc.stop();
		
		System.out.println("****  FP-Growth takes: " + time3 / 1000 + "s. ****");
	}

}
