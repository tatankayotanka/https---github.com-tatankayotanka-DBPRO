package util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import model.Transaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by teja on 26/04/15.
 */
public class AprioriData {
	
	public static final String[] TRANSACTIONS = new String[] {
		"1 3 4",
		"2 3 5",
		"1 2 3 5",
		"2 5",
		"1 3 5",
	};

	public static DataSet<String> getDefaultTextLineDataSet(ExecutionEnvironment env) {
		return env.fromElements(TRANSACTIONS);
	}
	
	
    public static DataSet<Transaction> getData(ExecutionEnvironment env) {
    	
        Transaction transaction1 = new Transaction(1, 1, Arrays.asList(1, 3, 4));
        Transaction transaction2 = new Transaction(1, 2, Arrays.asList(2, 3, 5));
        Transaction transaction3 = new Transaction(1, 3, Arrays.asList(1, 2, 3, 5));
        Transaction transaction4 = new Transaction(1, 4, Arrays.asList(2, 5));
        Transaction transaction5 = new Transaction(1, 5, Arrays.asList(1, 3, 5));
        Transaction transaction6 = new Transaction(2, 6, Arrays.asList(1, 3, 4, 5));
        Transaction transaction7 = new Transaction(2, 7, Arrays.asList(3, 5));
        Transaction transaction8 = new Transaction(2, 8, Arrays.asList(1, 4, 5));

        Transaction[] transactions = new Transaction[] {
                transaction1,
                transaction2,
                transaction3,
                transaction4,
                transaction5,
                transaction6,
                transaction7,
                transaction8
        };
        return env.fromElements(transactions);
    }
    
  
}
