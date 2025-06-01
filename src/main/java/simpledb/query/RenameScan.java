package simpledb.query;

public class RenameScan implements Scan {
    private final Scan s;
    private final String oldName, newName;

    public RenameScan(Scan s, String oldName, String newName) {
        this.s = s;
        this.oldName = oldName;
        this.newName = newName;
    }

    public void beforeFirst() {
        s.beforeFirst();
    }

    public boolean next() {
        return s.next();
    }

    public int getInt(String fldname) {
        if (fldname.equals(newName))
            return s.getInt(oldName);
        else
            return s.getInt(fldname);
    }

    public String getString(String fldname) {
        if (fldname.equals(newName))
            return s.getString(oldName);
        else
            return s.getString(fldname);
    }

    public Constant getVal(String fldname) {
        if (fldname.equals(newName))
            return s.getVal(oldName);
        else
            return s.getVal(fldname);
    }

    public boolean hasField(String fldname) {
        return s.hasField(fldname);
    }

    public void close() {
        s.close();
    }
}
