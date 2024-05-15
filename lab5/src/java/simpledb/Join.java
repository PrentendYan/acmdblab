package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate predicate;
    private DbIterator child1, child2;
    private TupleDesc schema1, schema2, joinSchema;
    private boolean useHashJoin = false;
    private HashEquiJoin hashJoin;
    private Tuple tuple1 = null;

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
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.predicate = p;
        this.child1 = child1;
        this.child2 = child2;
        this.schema1 = child1.getTupleDesc();
        this.schema2 = child2.getTupleDesc();
        this.joinSchema = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
        useHashJoin = predicate.getOperator().equals(Predicate.Op.EQUALS);
        if (useHashJoin) this.hashJoin = new HashEquiJoin(p, child1, child2);
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return predicate;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return schema1.getFieldName(predicate.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return schema2.getFieldName(predicate.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.joinSchema;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        if (useHashJoin) hashJoin.open();
        else {
            child1.open();
            child2.open();
        }
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        if (useHashJoin) hashJoin.close();
        else {
            child1.close();
            child2.close();
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (useHashJoin) {
            return this.hashJoin.fetchNext();
        }

        while (tuple1 != null || child1.hasNext()) {
            if (tuple1 == null) {
                tuple1 = child1.next();
            }

            while (child2.hasNext()) {
                Tuple tuple2 = child2.next();
                if (predicate.filter(tuple1, tuple2)) {
                    return Tuple.merge(tuple1, tuple2, schema1, schema2, joinSchema);
                }
            }

            tuple1 = child1.hasNext() ? child1.next() : null;
            child2.rewind();
        }

        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] {child1,child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        if (useHashJoin) hashJoin.setChildren(children);
        child1 = children[0];
        child2 = children[1];
    }

}
