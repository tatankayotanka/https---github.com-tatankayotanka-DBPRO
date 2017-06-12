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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;

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

import freqitems.util.ItemSet;

import static java.nio.file.Files.*;


/**
 * @author Ariane Ziehn
 *         <p>
 *         Run with:
 *         ./OnlineRetail/OnlineRetail.csv 2 5
 */
public class OnlineRetail2 {

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

//    String dataPath = "/projects/Flink/dfki-tutorial/OnlineRetail/OnlineRetail.csv";

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
                .includeFields("11000000").ignoreFirstLine()
                .types(String.class, String.class)
                .filter(new ReadInFilter());

        System.out.println("csvInput.count=" + csvInput.count());
        List<Tuple2<String, String>> list = csvInput.collect();

       // System.out.println("csvInput firstline=" + list.get(0));
       // System.out.println("csvInput secondline=" + list.get(1));

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
                .map(new AssignUniqueId()).setParallelism(1);
        System.out.println("\n****** Number of unique items:" + itemsSet.count());
        System.out.println("first item " + itemsSet.collect().get(0));


     /*
      * In the next step we use dataset itemset above to create a List of
      * Integers containing all items of one transaction in a time sequence
      * order groups of InvoiceNo > .group(0) to map the unique identifiers
      * from itemSet to the String values from csvInput we can use the method
      * from BasketAnalysis, but need to edit it for time relevant sorting
      */

        DataSet<List<Integer>> transactionList = csvInput.groupBy(0)
                .reduceGroup(new GroupItemsInListPerBill())
                .withBroadcastSet(itemsSet, "itemSet");
        System.out.println("transactionList");
        transactionList.print();
        System.out.println("\n****** Number of transactions:" + transactionList.count());


        DataSet<List<String>> transactionList2 = csvInput.groupBy(0)
                .reduceGroup(new GroupItemsInListPerBill2())
                .withBroadcastSet(itemsSet, "itemSet");
        List<List<String>> ll = transactionList2.collect();
        Path csvFileOut = Paths.get("./OnlineRetail/test.csv");
        StringBuilder sb = new StringBuilder();

        for (List<String> ls : ll) {
            String listString = ls.toString();
            sb.append(listString.substring(1, listString.length()-1));
            sb.append("\n");
        }
        try{
            PrintWriter writer = new PrintWriter("output.csv", "UTF-8");
            writer.println(sb.toString().replace(",", ""));
            writer.close();
        } catch (IOException e) {
        }
        /*try (BufferedWriter writer = Files.newBufferedWriter(csvFileOut, StandardCharsets.UTF_8, StandardOpenOption.WRITE)) {
            for (List<String> ls : ll) {
                for (String s : ls)
                    sb.append(s).append(" ");
                    sb.
                    sb.append("\n");
                    writer.write(sb.toString());
            }
        }*/

        //transactionList2.writeAsText("./OnlineRetail/test.csv.txt");
        // transactionList2.print();
        System.out.println("\n****** Number of transactions2:" + transactionList2.count());


        /**
         * MINING PART
         */

     /*
      * send transactions to Apriori with .mine(DataSet<List<Integer>,
      * MinimumSupport, Iterations) no changes to BasketAnalysis
      */
       // DataSet<ItemSet> items = Apriori.mine(transactionList, minSupport, numIterations);

        /**
         * POST PROCESSING
         */

     /*
      * For the final result the last transformation is the map back of the
      * StockCodes no changes to BasketAnalysis
      */

       /* DataSet<List<String>> transactionListMapped = items.flatMap(
                new MapBackTransactions1()).withBroadcastSet(itemsSet,
                "itemSet");

        transactionListMapped.print();*/

    } // end main method

    public static class MapBackTransactions extends
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

    public static class GroupItemsInListPerBill
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
            List<Integer> items = new ArrayList<Integer>();
            //Tuple2<String,List<Integer>> transactionIdWithItemsset;

            // f0 f1 f2
            // Tuple3<transaction number, product, timestamp>
            for (Tuple2<String, String> trans : transactions) {
                if (itemSetMap.containsKey(trans.f1))
                    items.add(itemSetMap.get(trans.f1));


                // System.out.println("   " + trans.f0 + " " + trans.f2 + " " +
                // itemSetMap.get(trans.f2));
            }

            if (!items.isEmpty())
                out.collect(items);
        }
    }

    public static class GroupItemsInListPerBill2
            extends
            RichGroupReduceFunction<Tuple2<String, String>, List<String>> {

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
                Collector<List<String>> out) {

            // transactions contains all the transactions with the same
            // InvoiceNo
            // collect here the products of all those transactions and map them
            // into integers
            // also keep the timestamp(InvoiceDate)to sort them afterwards
            //  Tuple2<String,List<String>> result = null;
            List<String> items = new ArrayList<String>();
            //Tuple2<String,List<Integer>> transactionIdWithItemsset;

            // f0 f1 f2
            // Tuple3<transaction number, product, timestamp>
            for (Tuple2<String, String> trans : transactions) {
                String invoiceNo = trans.f0;
                if (itemSetMap.containsKey(trans.f1))

                    items.add(trans.f1);


                // System.out.println("   " + trans.f0 + " " + trans.f2 + " " +
                // itemSetMap.get(trans.f2));
                //result=new Tuple2<>(invoiceNo,items);
            }


            if (!items.isEmpty())
                out.collect(items);
        }
    }

    public static final class WriteOut {

    }

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


    public static final class ReadInFilter implements
            FilterFunction<Tuple2<String, String>> {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean filter(Tuple2<String, String> value) {
            // System.out.println("f0: "+ value.f0+ "f1: "+ value.f1+"f2: "+ value.f2);
            if (value.f0 != null && value.f0 != "")
                if (value.f1 != null && value.f1 != "")
                    if (value.f1.matches("[0-9:]+"))
                        return true;
            return false;
        }
    }
}

