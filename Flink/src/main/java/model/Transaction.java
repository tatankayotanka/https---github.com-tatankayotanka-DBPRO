package model;

/**
 * Created by teja on 28/04/15.
 */

import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Using the data structure from the original source code for Hadoop
 */
public class Transaction implements Serializable
{
    public int cid, tid;

    // FIXME: This should ideally be a list but serialization of a list fails on Flink for some unknown reason
    public String items;
    
    // this list can be made (just once) when creating the Transaction  ???
    // not sure if it helps to have a copy in a list...
    List<Integer> itemsList;

    public Transaction() {}

    public Transaction(int cid, int tid, List<Integer> items) {
        this.cid = cid;
        this.tid = tid;
        this.items = StringUtils.join(items, ",");
        this.itemsList = this.getItemsList();
    }
    
    public Transaction(int cid, int tid, String itemsCommaSeparated, List<Integer> itemsTransList) {
        this.cid = cid;
        this.tid = tid;
        this.items = itemsCommaSeparated;
        this.itemsList = itemsTransList;
    }

    @Override
    public String toString() {
        return "Transaction [tid=" + tid + ", cid=" + cid + ", items=" + items + "]";
    }

    public int getTid() {
        return tid;
    }

    public void setTid(int tid) {
        this.tid = tid;
    }

    public int getCid() {
        return cid;
    }

    public void setCid(int cid) {
        this.cid = cid;
    }

    public List<Integer> getItems() {
    	return itemsList;
    }
    
    public List<Integer> getItemsList() {
        String[] items = this.items.split(",");

        List<Integer> results = new ArrayList<Integer>();

        for (int i = 0; i < items.length; i++) {
            try {
                results.add(Integer.parseInt(items[i]));
            } catch (NumberFormatException nfe) {};
        }

        return results;
    }
}