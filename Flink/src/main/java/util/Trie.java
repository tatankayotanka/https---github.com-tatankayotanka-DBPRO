package util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import util.ItemSet;

/**
 * from https://community.oracle.com/thread/2070706
 * adapted to sorted lists of integers:
 * 
 * @author marcela
 *
 */
public class Trie {

   private TrieNode root;
   
   /**
    * Constructor
    */
   public Trie()
   {
      root = new TrieNode();
   }
   
   /**
    * Adds a word to the Trie
    * @param word
    */
   public void addTransaction(List<Integer> trans)
   {      
	   root.addTransaction(trans);  // <-- transactions are already sorted
	   //root.printChildrenList();
   }
   
   public void walkTransaction(List<Integer> trans, int numLevels, Collector <ItemSet> out)
   {
	   root.walkTransaction(trans, numLevels, out);  // <-- transactions are already sorted
   }
   
   public void walkTransactionCounting(List<Integer> trans, int numLevels)
   {
	   root.walkTransactionCounting(trans, numLevels);  // <-- transactions are already sorted
	   //root.printChildrenList();	   
   }
   public void walkTransactionTuple(List<Integer> trans, int numLevels, Collector <Tuple4<Integer, Integer, Integer, Integer>> out)
   {
	   root.walkTransactionTuple(trans, numLevels, out);  // <-- transactions are already sorted
	   //root.printChildrenList();	   
   }
   
   public void addTransaction(int[] trans)
   {
	   root.addTransaction(trans, 0);  // <-- transactions are already sorted
	                                   // <-- first time adding a transaction or list the index is 0
	   //root.printChildrenList();
   }
   
   public void printTrie(){
	   root.printTransactions();
   }
   
   public void collectFrequentItemSets(Collector <ItemSet> out){
	   root.collectFrequentItemSets(out);
   }
   
   public List<Integer> getFirstLevelItems(){
	   return root.getChildrenList();
   }
   
   public List<Integer> getSecondLevelItems(int item){
	   // first find the item in first level and return its children
	   TrieNode trie = root.findTrieNode(item);	   
	   return trie.getChildrenList();
   }
   
   public List<Integer> getThirdLevelItems(int item1, int item2){
	   // first find the item in first level and return its children
	   TrieNode trie1 = root.findTrieNode(item1);
	   TrieNode trie2 = trie1.findTrieNode(item2);
	   return trie2.getChildrenList();
   }
   public List<Integer> getFourthLevelItems(int item1, int item2, int item3){
	   // first find the item in first level and return its children
	   TrieNode trie1 = root.findTrieNode(item1);
	   TrieNode trie2 = trie1.findTrieNode(item2);
	   TrieNode trie3 = trie2.findTrieNode(item3);
	   return trie3.getChildrenList();
   }
   
   public static void main(String[] args) throws Exception {
	  
   	
   	int minSupport = 3;
   	String dataPath = "/projects/data/FrequentItemsetMining/simple_data.dat";
    	
   	//int minSupport = 500;
   	//String dataPath = "/projects/data/FrequentItemsetMining/T10I4D100K.dat";
   	
   	//int minSupport = 5940;
   	//String dataPath = "/projects/data/FrequentItemsetMining/kosarak.dat";
   	   	
   	//int minSupport = 2100;
   	//String dataPath = "/projects/data/FrequentItemsetMining/T40I10D100K.dat";
   	
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    
    		
    // get input data
    DataSet<String> text = getTextDataSet(env, dataPath);
       
    // get the list of transactions, every transaction will be kept in a List<Integer>
    DataSet<List<Integer>> transactionList = text.flatMap(new LineSplitter());
        
    transactionList.print();
    
    //Trie t = new Trie();
    
    DataSet<Trie> t = transactionList.reduceGroup(new GenerateTrie());
    
    t.print();
    
	   
   }
   
	public static class GenerateTrie implements GroupReduceFunction<List<Integer>, Trie> {		
		@Override
		public void reduce(Iterable<List<Integer>> list_trans, Collector <Trie> out) {
			Trie t = new Trie();
			for (List<Integer> trans : list_trans) {			   
			   t.addTransaction(trans);   			      
			}			
			t.printTrie();			
			out.collect(t);
		}			
	}
	
	private static DataSet<String> getTextDataSet(ExecutionEnvironment env, String dataPath) {		
		if(dataPath != null) {
			// read the text file from resources path
			return env.readTextFile(dataPath);
		} else {
			// get default test text data
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
	          //TODO: check in which cases this might help
	            Collections.sort(itemsList); // sorting the list of items in each transaction
	            out.collect(itemsList);            
	        }
	    }
   
}


