package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate predicate;
    private DbIterator child1, child2;
    private TupleDesc schema1, schema2, joinSchema;
    private Map<Field, ArrayList<Tuple>> hashBlocks = new ConcurrentHashMap<>();
    private Tuple tuple1;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.predicate = p;
        this.child1 = child1;
        this.child2 = child2;
        this.schema1 = child1.getTupleDesc();
        this.schema2 = child2.getTupleDesc();
        this.joinSchema = TupleDesc.merge(schema1, schema2);
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return joinSchema;
    }
    
    public String getJoinField1Name()
    {
        // some code goes here
        return schema1.getFieldName(predicate.getField1());
    }

    public String getJoinField2Name()
    {
        // some code goes here
        return schema2.getFieldName(predicate.getField2());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child1.open();
        child2.open();

        hashBlocks.clear();

        while (child2.hasNext()) {
            Tuple tuple2 = child2.next();
            Field hashKey = tuple2.getField(predicate.getField2());
            hashBlocks.computeIfAbsent(hashKey, k -> new ArrayList<>()).add(tuple2);
        }

        child2.rewind();

        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child1.close();
        child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
    }

    transient Iterator<Tuple> listIt = null;

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, there will be two copies of the join attribute in
     * the results. (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while (listIt != null && listIt.hasNext()) {
            Tuple tuple2 = listIt.next();
            return Tuple.merge(tuple1, tuple2, schema1, schema2, joinSchema);
        }

        while (child1.hasNext()) {
            tuple1 = child1.next();
            Field hashKey = tuple1.getField(predicate.getField1());
            ArrayList<Tuple> matchedBlock = hashBlocks.getOrDefault(hashKey, null);

            if (matchedBlock != null) {
                listIt = matchedBlock.iterator();
                Tuple result = fetchNext();
                if (result != null) {
                    return result;
                }
            }
        }

        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child1, child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child1 = children[0];
        child2 = children[1];
    }
    
}
