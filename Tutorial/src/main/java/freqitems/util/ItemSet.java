package freqitems.util;

import java.util.ArrayList;
import java.util.List;

public class ItemSet {
	
	public List<Integer> itemset = new ArrayList<Integer>();
	private int count;

	// Public constructor to make it a Flink POJO
	public ItemSet() {
		
	}
	
	public ItemSet(List<Integer> itemsList, int num) {
		itemset = itemsList;
		count = num;
	}        
	
	public List<Integer> getItemSetList() {
		return this.itemset;
	}
	
	public String getItemSetId() {
		return this.itemset.toString();
	}
	
	public int getCount(){
		return this.count;
	}
	
	@Override
	public String toString() {
		String str = "(";
		for(int item: itemset)
			str = str + item + " ";
		str = str + "): " + count;
		return str;
	}
}

