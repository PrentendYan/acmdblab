package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField, aField;
    private Type gbFieldType;
    private Op operator;
    private TupleDesc schema;

    private Map<Field, Integer> aggregates = new HashMap<>();
    private Map<Field, Integer> counts = new HashMap<>();


    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.operator = what;
        this.aggregates = new HashMap<>();
        this.counts = new HashMap<>();

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
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupField = gbField == Aggregator.NO_GROUPING ? null : tup.getField(gbField);
        int tvalue = ((IntField) tup.getField(aField)).getValue();

        Integer ovalue = aggregates.getOrDefault(groupField, null);
        int count = counts.getOrDefault(groupField, 0);

        int newValue;
        switch (operator) {
            case MIN:
                newValue = ovalue == null ? tvalue : Math.min(ovalue, tvalue);
                break;
            case MAX:
                newValue = ovalue == null ? tvalue : Math.max(ovalue, tvalue);
                break;
            case SUM:
            case AVG:
                newValue = ovalue == null ? tvalue : ovalue + tvalue;
                break;
            case COUNT:
                newValue = count + 1;
                break;
            default:
                throw new IllegalArgumentException("Unknown operator: " + operator);
        }

        aggregates.put(groupField, newValue);
        counts.put(groupField, count + 1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();

        for (Map.Entry<Field, Integer> entry : aggregates.entrySet()) {
            Field group = entry.getKey();
            int value = entry.getValue();

            if (operator == Op.AVG) {
                int count = counts.getOrDefault(group, 1);
                value /= count;
            }

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
