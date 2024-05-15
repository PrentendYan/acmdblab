package simpledb;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField, aField;
    private Type gbFieldType;
    private Op operator;
    private TupleDesc schema;

    private Map<Field, Integer> counts = new HashMap<>();

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Only COUNT operator is supported for StringAggregator.");
        }

        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.operator = what;

        Type[] types;
        String[] names;
        if (gbfield == Aggregator.NO_GROUPING) {
            types = new Type[]{Type.INT_TYPE};
            names = new String[]{"aggregateValue"};
        } else {
            types = new Type[]{gbFieldType, Type.INT_TYPE};
            names = new String[]{"groupValue", "aggregateValue"};
        }
        this.schema = new TupleDesc(types, names);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupField = gbField == Aggregator.NO_GROUPING ? null : tup.getField(gbField);

        int count = counts.getOrDefault(groupField, 0);
        counts.put(groupField, count + 1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();

        for (Map.Entry<Field, Integer> entry : counts.entrySet()) {
            Field group = entry.getKey();
            int value = entry.getValue();

            Tuple tuple = new Tuple(schema);
            if (gbField == Aggregator.NO_GROUPING) {
                tuple.setField(0, new IntField(value));
            } else {
                tuple.setField(0, group);
                tuple.setField(1, new IntField(value));
            }

            tuples.add(tuple);
        }

        return new TupleIterator(schema, tuples);
    }

}
