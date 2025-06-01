package simpledb.query;

import java.util.*;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.metadata.MetadataMgr;
import simpledb.record.*;

public class TestOperator {
    private static SimpleDB db = new SimpleDB("testoperator");
    private static String tblname = "T";

    public static void main(String[] args) {
        createTable();
        query1();
        query2();
    }

    private static void createTable() {
        MetadataMgr mdm = db.mdMgr();
        Transaction tx  = db.newTx();
        Layout layout = mdm.getLayout(tblname, tx);
        if (layout.slotSize() > 0) {
            tx.commit();
            return;  // Use existing table
        }

        // Create a table T(A int, B varchar(15));
        Schema sch = new Schema();
        sch.addIntField("A");
        sch.addStringField("B", 15);
        mdm.createTable(tblname, sch, tx);
        layout = mdm.getLayout(tblname, tx);

        // Populate the table
        UpdateScan s = new TableScan(tx, tblname, layout);
        for (int i=0; i<300; i++) {
            s.insert();
            s.setInt("A", i);
            s.setString("B", "b"+(i%20));
        }
        s.close();
        tx.commit();
    }

    private static void query1() {
        // Query 1: Print the A-values of records
        // having the same B-value as record 33.

        Transaction tx = db.newTx();
        MetadataMgr mdm = db.mdMgr();
        Layout layout = mdm.getLayout(tblname, tx);

        // A predicate corresponding to "A = 33".
        Expression lhs = new Expression("A");
        Constant c = new Constant(33);
        Expression rhs = new Expression(c);
        Term t = new Term(lhs, rhs);
        Predicate selectpred = new Predicate(t);

        // A predicate corresponding to "B = Bof33"
        Expression lhs2 = new Expression("B");
        Expression rhs2 = new Expression("Bof33");
        Term t2 = new Term(lhs2, rhs2);
        Predicate joinpred = new Predicate(t2);

        Scan s1 = new TableScan(tx, tblname, layout);
        Scan s2 = new SelectScan(s1, selectpred);
        List<String> flds = Arrays.asList("B");
        Scan s3 = new ProjectScan(s2, flds);
        Scan s4 = new RenameScan(s3, "B", "Bof33");
        Scan s5 = new TableScan(tx, tblname, layout);
        Scan s6 = new ProductScan(s5, s4);
        Scan s7 = new SelectScan(s6, joinpred);

        System.out.println("Here are the records that have the same B-value as record 33:");
        ArrayList<Integer> list = new ArrayList<>();
        while (s7.next())
            list.add(s7.getInt("A"));
//        System.out.print(s7.getInt("A") + " ");

//        Collections.sort(list);
        for (int num : list)
            System.out.print(num + " ");

        System.out.println("\n");
        s7.close();
        tx.commit();
    }

    private static void query2() {
        // Query 2: Print the A-values of records
        // whose B-values are either "b1" or "b9".

        Transaction tx = db.newTx();
        MetadataMgr mdm = db.mdMgr();
        Layout layout = mdm.getLayout(tblname, tx);
        Scan s1 = new TableScan(tx, tblname, layout);
        Scan s2 = new TableScan(tx, tblname, layout);
        // A select predicate for "B = 'b1' "
        Expression lhs = new Expression("B");
        Constant c = new Constant("b1");
        Expression rhs = new Expression(c);
        Term t = new Term(lhs, rhs);
        Predicate pred1 = new Predicate(t);

        // A select predicate for "B = 'b9' "
        Expression lhs2 = new Expression("B");
        Constant c2 = new Constant("b9");
        Expression rhs2 = new Expression(c2);
        Term t2 = new Term(lhs2, rhs2);
        Predicate pred2 = new Predicate(t2);

        Scan s3 = new SelectScan(s1, pred1);
        Scan s4 = new SelectScan(s2, pred2);
        Scan s5 = new UnionScan(s3, s4);

        System.out.println("Here are the records that have the B-value 'b1' or 'b9': ");

        ArrayList<Integer> list = new ArrayList<>();
        while (s5.next())
            list.add(s5.getInt("A"));
//        System.out.print(s5.getInt("A") + " ");

//        Collections.sort(list);
        for (int num : list)
            System.out.print(num + " ");

        System.out.println("\n");
        s5.close();
        tx.commit();
    }
}

/* Output from correct implementation

creating new database
transaction 1 committed
transaction 2 committed
Here are the records that have the same B-value as record 33:
13 33 53 73 93 113 133 153 173 193 213 233 253 273 293

transaction 3 committed
Here are the records that have the B-value 'b1' or 'b9':
1 21 41 61 81 101 121 141 161 181 201 221 241 261 281 9 29 49 69 89 109 129 149 169 189 209 229 249 269 289

transaction 4 committed
 */
