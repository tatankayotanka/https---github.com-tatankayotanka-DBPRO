package freqitems.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import freqitems.util.ItemSet;

/**
 * from https://community.oracle.com/thread/2070706
 * adapted to sorted lists of integers: transactions
 * @author marcela
 *
 */
public class TrieNode
{
   private TrieNode parent;
   private List<TrieNode> children; // List = new ArrayList<Integer>(); 
   private List<Integer> childrenList; // keep in a list the items this node contains to easy search
   private boolean isLeaf;      // Quick way to check if any children exist
   private boolean isTrans;     // Does this node represent the last character of a word
   private int item;            // The int/item this node represents
   private int counter;         // how many times this branch appears, added just when walking levels with transactions
   
   
   /**
    * Constructor for top level root node.
    */
   public TrieNode()
   {
      children = new ArrayList<TrieNode>();  // <-- this should be the number of possible items
      childrenList = new ArrayList<Integer>();
      isLeaf = true;
      isTrans = false;
      parent = null;
      counter = 0;
   }
   
   public List<Integer> getChildrenList(){
	   return this.childrenList;
   }

   /**
    * Constructor for child node.
    */
   public TrieNode(int item)
   {
      this();
      this.item = item;
   }
   public void increaseCounter()
   {
      this.counter++;
   }
   
   /**
    * find the node that contains this item
    * return null if item not found
    * @param item
    * @return
    */
   public TrieNode findTrieNode(int item)
   {
      for(TrieNode node: this.children){
    	 if(node.item == item)
    		 return node;
      }
      return null;
   }
   
   /**
    * Add to the Trie the items of the transaction one by one recursively
    * @param word the word to add
    */
   protected void addTransaction(List<Integer> trans)
   {
      isLeaf = false;      
      int intPos = trans.get(0);
      TrieNode node = null;      
      
      if (!this.childrenList.contains(intPos)) {
    	  node = new TrieNode(intPos);
    	  node.parent = this;
    	  node.item = intPos;
    	  this.children.add(node);  
    	  this.childrenList.add(intPos);
    	  //System.out.println("  add item: " + intPos);
      } else {
    	  //System.out.println("  item in Trie: " + intPos);
    	  // if the item exists in childrenList, then find the node that contains this item
    	  node = this.findTrieNode(intPos);    	  
      }
      // if this item already exist then remove it and continue
      // having counters we would need just to add a counter
      trans.remove(0);
      
      if (trans.size() > 0) {
    	  if(node != null) {
    		node.addTransaction(trans);
    	  }
      } else {
    	  if(node != null) {
    		node.isTrans = true;  
    	  }
      }
   }
   

   /**
    * The purpose of this function is to generate candidates of certain length, based on the Trie generated in the previous iteration
    * Having already a Trie with a certain number of levels (ex. in the second iteration the Trie contains 2 levels)
    * this function walk the trie with the elements of the transaction and add leaves to the posible paths
    * the output is already collected in the form of ItemSet
    * @param trans
    * @param numLevels
    * @param out
    */
   protected void walkTransaction(List<Integer> trans, int numLevels, Collector <ItemSet> out)
   {
      int intPos = trans.get(0);  // get the first in the list
      TrieNode node = null;      
      int currentLevel = numLevels;
      int downLevel = numLevels;
            
      if (trans.size() > 1) {
    	  // check if children of current level contains this item
    	  if (this.childrenList.contains(intPos)) {
    		  // the children nodes contain this item, so go one level down
    		  downLevel = downLevel-1;  // go one level down
    		  trans.remove(0);  // remove the first in the list
    		  node = this.findTrieNode(intPos);
    	  
    		  if( downLevel > 1) { 
    			  // IMPORTANT: make a copy of the sublist with which the possible candidates can be generated
    			  List<Integer> subtrans = new ArrayList<Integer>();
    			  for( int item : trans)
    				  subtrans.add(item); 			  
    			  node.walkTransaction(subtrans, downLevel, out);   			  
    		  } else {
    			  // generate candidates, with the path so far in combination with any of the 
    			  // items left in the trans
				  List<Integer> p = new ArrayList<Integer>();
				  node.getParentsList(p);
				  
    			  for( int item : trans){
    				  List<Integer> candidate = new ArrayList<Integer>();
    				  //System.out.print("     new itemset: ");
    				  for(int i=p.size()-1; i>=0; i--){
    					  candidate.add(p.get(i));
    					  //System.out.print(p.get(i) + " ");   					  
    				  }
    				  //System.out.println(item);
    				  candidate.add(item);
    				  out.collect(new ItemSet(candidate, 1));    				      				
    			  }
    		  }
    		  this.walkTransaction(trans, currentLevel, out);
    		  
    	  } else { // item is not in this level of the trie, delete this item and check if next item is in this level    		 
    		  trans.remove(0);  // remove the first in the list, but stay in the same level              	  
    		  this.walkTransaction(trans, currentLevel, out);    		 
    	  }   	  
      } // if items left in transaction  
   }

   /**
    * The purpose of this function is to generate candidates of certain length, based on the Trie generated in the previous iteration
    * Having already a Trie with a certain number of levels (ex. in the second iteration the Trie contains 2 levels)
    * this function walk the trie with the elements of the transaction, add leaves to the possible paths and increase their counters
    * this function just update the leaves of the three: adding new leaves to the possible paths or incrementing the counters
    * of the existing leaves.
    * @param trans
    * @param numLevels
    */
   protected void walkTransactionCounting(List<Integer> trans, int numLevels)
   {
      int intPos = trans.get(0);  // get the first in the list
      TrieNode node = null;  
      TrieNode newNode = null;
      int currentLevel = numLevels;
      int downLevel = numLevels;
            
      if (trans.size() > 1) {
    	  // check if children of current level contains this item
    	  if (this.childrenList.contains(intPos)) {
    		  // the children nodes contain this item, so go one level down
    		  downLevel = downLevel-1;  // go one level down
    		  trans.remove(0);  // remove the first in the list
    		  node = this.findTrieNode(intPos);
    	  
    		  if( downLevel > 1) { 
    			  // IMPORTANT: make a copy of the sublist with which the possible candidates can be generated
    			  List<Integer> subtrans = new ArrayList<Integer>();
    			  for( int item : trans)
    				  subtrans.add(item); 			  
    			  node.walkTransactionCounting(subtrans, downLevel);   			  
    		  } else {
    			  // generate candidates, with the path so far in combination with any of the 
    			  // items left in the trans
				//  List<Integer> p = new ArrayList<Integer>();
				//  node.getParentsList(p);
				  
				  // since here we are adding nodes to current node then set its flag as not Leaf
    			  node.isLeaf = false;
				  node.isTrans = true;
				  
    			  for( int item : trans){    				
    				  //System.out.println("    adding node: " + item);
    				  // check first if the node is already in the trie
    				  // add leaves to the node found
    				  newNode = node.findTrieNode(item);
    				  if(newNode == null) {    		
    					  newNode = new TrieNode(item);
    					  newNode.parent = node;
    					  newNode.item = item;
    					  node.children.add(newNode);  
    					  node.childrenList.add(item); 
    					  newNode.increaseCounter();
    				  } else {
    					  // if the     					  
    					  newNode.increaseCounter();
    					  //System.out.println("    increasing counter of node: " + item + " count=" + newNode.counter);
    				  }   				      				  
    			  }
    		  }
    		  this.walkTransactionCounting(trans, currentLevel);
    		  
    	  } else { // item is not in this level of the trie, delete this item and check if next item is in this level    		 
    		  trans.remove(0);  // remove the first in the list, but stay in the same level              	  
    		  this.walkTransactionCounting(trans, currentLevel);    		 
    	  }   	  
      } // if items left in transaction  
   }
   
   
   
   /**
    * The same as walkTransaction but the result is a Tuple
    * @param trans
    * @param numLevels
    * @param out
    */
   protected void walkTransactionTuple(List<Integer> trans, int numLevels, Collector <Tuple4<Integer,Integer, Integer, Integer>> out)
   {
	   
	  /*System.out.print("level=" + numLevels + ": ");
	  for( int item : trans)
		 System.out.print(item + " ");
	  System.out.println(); */
	  
      int intPos = trans.get(0);  // get the first in the list
      TrieNode node = null;      
      int currentLevel = numLevels;
      int downLevel = numLevels;
            
      if (trans.size() > 1) {
    	  // check if children of current level contains this item
    	  if (this.childrenList.contains(intPos)) {
    		  // the children nodes contain this item, so go one level down
    		  downLevel = downLevel-1;  // go one level down
    		  trans.remove(0);  // remove the first in the list
    		  node = this.findTrieNode(intPos);
    	  
    		  if( downLevel > 1) {  
    			  // IMPORTANT: make a copy of the sublist with which the possible candidates can be generated
    			  List<Integer> subtrans = new ArrayList<Integer>();
    			  for( int item : trans)
    				  subtrans.add(item);
    			  node.walkTransactionTuple(subtrans, downLevel, out);   			  
    		  } else {
    			  // generate candidates, with the path so far in combination with any of the 
    			  // items left in the trans
				  List<Integer> p = new ArrayList<Integer>();
				  node.getParentsList(p);
				  
    			  for( int item : trans){
    				  List<Integer> candidate = new ArrayList<Integer>();
    				//  System.out.print("     new itemset: ");
    				  for(int i=p.size()-1; i>=0; i--){
    					  candidate.add(p.get(i));
    					//  System.out.print(p.get(i) + " ");   					  
    				  }
    				  //System.out.println(item);
    				  candidate.add(item);
    				  //out.collect(new ItemSet(candidate, 1));
    				  out.collect(new Tuple4<Integer, Integer, Integer, Integer>
			             (candidate.get(0), candidate.get(1), candidate.get(2), 1));	
    			  }
    		  }    		 
    		  this.walkTransactionTuple(trans, currentLevel, out);
    		  
    	  } else { // item is not in this level of the trie, delete this item and check if next item is in this level    		 
    		  trans.remove(0);  // remove the first in the list, but stay in the same level              	  
    		  this.walkTransactionTuple(trans, currentLevel, out);    		 
    	  }   	  
      } // if items left in transaction  
   }

   
   /**
    * This function is used to create a Trie using transactions.
    * Here is used to create a Trie with the frequent items sets found in a previous iteration
    * once the Trie is created it can be used to generate candidates for the next iteration.
    * @param trans
    * @param intPos
    */
   protected void addTransaction(int[] trans, int intPos)
   {
      isLeaf = false;      
      TrieNode node = null;
      
      // first we need to check if the children (item) exist in this level
      // if not create a new one      
      if (!childrenList.contains(trans[intPos])) {
    	  node = new TrieNode(trans[intPos]);
    	  node.parent = this;
    	  node.item = trans[intPos];
    	  children.add(node);
    	  childrenList.add(trans[intPos]);
    	  //System.out.println("  add item: " + trans[intPos]);
      } else {
    	  //System.out.println("  item in Trie: " + trans[intPos]);
    	  // if the item exists in childrenList, then find the node that contains this item
    	  node = this.findTrieNode(trans[intPos]);    	
      }
      // if this item already exist then just increase the index of trans array
      // having counters we would need just to add a counter
      int nextIndex = intPos + 1;
      
      if (nextIndex < trans.length ) {
    	  if (node != null) {
    		  node.addTransaction(trans, nextIndex);
    	  }
      } else {
    	  if (node != null) {
    		  node.isTrans = true;
    	  }    	  
      }
   }
   

   /**
    * print the leaves including the parents path
    */
   public void printTransactions()
   {      
      if (!isLeaf) {
    	  //System.out.print(this.item + " ");
    	  for(TrieNode node: children ) {
    		 node.printTransactions(); 
    	  }
      } else {
    	  List<Integer> p = new ArrayList<Integer>();		  
    	  this.getParentsList(p);
    	  
    	  for(int i=p.size()-1; i>=0; i--){
			 System.out.print(p.get(i) + " ");   					  
		  }
		  //System.out.println(item);    	  
    	  //System.out.println(this.item + " " + "counter: " + counter + " end");
    	  System.out.println(" " + "counter: " + counter + " end");
      }
   }
   
   /**
    * This function is used after walkTransactionTuple, once all the transaction have been traversed the trie and new
    * itemset are generated, this function collect them in a collector, including the counters
    * @param out
    */
   public void collectFrequentItemSets(Collector <ItemSet> out)
   {      
      if (!isLeaf) {
    	  //System.out.print(this.item + " ");
    	  for(TrieNode node: children ) {
    		 node.collectFrequentItemSets(out); 
    	  }
      } else {
    	  List<Integer> p = new ArrayList<Integer>();		  
    	  this.getParentsList(p);
    	     	  
    	  List<Integer> candidate = new ArrayList<Integer>();
		  //System.out.print("     new itemset: ");
		  for(int i=p.size()-1; i>=0; i--){
			  candidate.add(p.get(i));
			  //System.out.print(p.get(i) + " ");   					  
		  }
		  //System.out.println(" " + "counter: " + counter + " end");
		  out.collect(new ItemSet(candidate, counter));
    	  
      }
   }
   
   /**
    * Returns in p the parent's list of a node or leaf
    * @param p
    */
   public void getParentsList(List<Integer> p){
	   if (parent!= null) {
		   p.add(item);
		   parent.getParentsList(p);
	   } 
   }
   
   /**
    * Print recursively the children of every node
    */
   public void printChildrenList()
   {      
	   System.out.print("  childrenList of: " + this.item + " :");
	   for(Integer item: childrenList ) {
		   System.out.print(item + " ");  
	   }
	   System.out.println();
	   for(TrieNode node: children ) {
    	 node.printChildrenList(); 
       }
      
   }
   
   

}