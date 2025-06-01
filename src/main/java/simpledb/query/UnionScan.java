package simpledb.query;

public class UnionScan implements Scan {
    private Scan s1, s2;
    private int turn = 1;

    /**
     * Create a union scan having the two underlying scans.
     * @param s1 the LHS scan
     * @param s2 the RHS scan
     */
    public UnionScan(Scan s1, Scan s2) {
        this.s1 = s1;
        this.s2 = s2;
    }

    public void beforeFirst() {
        s1.beforeFirst();
        s2.beforeFirst();
    }

    public boolean next() {
        if (s1.next())
            return true;
        else {
            turn = 2;
            return s2.next();
        }
    }

    public int getInt(String fldname) {
        if (turn == 1)
            return s1.getInt(fldname);
        else
            return s2.getInt(fldname);
    }

    public String getString(String fldname) {
        if (turn == 1)
            return s1.getString(fldname);
        else
            return s2.getString(fldname);
    }

    public Constant getVal(String fldname) {
        if (turn == 1)
            return s1.getVal(fldname);
        else
            return s2.getVal(fldname);
    }

    public boolean hasField(String fldname) {
        return s1.hasField(fldname) || s2.hasField(fldname);
    }

    public void close() {
        s1.close();
        s2.close();
    }
}
