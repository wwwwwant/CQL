package soton.want.calcite.operators.physic;

import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.log4j.Logger;
import soton.want.calcite.operators.Tuple;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

/**
 * @author want
 */
public class GroupByOperator extends UnaryOperator<Aggregate> {
    private static final Logger LOGGER = Logger.getLogger(GroupByOperator.class);

    public GroupByOperator(Aggregate logicalNode, Operator child) {
        super(logicalNode, (AbstractOperator) child);
        groupSet = logicalNode.getGroupSet();
        aggCallList = logicalNode.getAggCallList();
        init();
    }

    private ImmutableBitSet groupSet;
    private List<AggregateCall> aggCallList;
    private List<BaseAggFunction> aggFunctions = new ArrayList<>();
    private HashMap<GroupKey, Tuple> res = new HashMap<>();


    private void init() {

        for (AggregateCall call : aggCallList) {
            SqlAggFunction aggFunction = call.getAggregation();

            if (aggFunction.equals(SqlStdOperatorTable.COUNT)) {
                aggFunctions.add(new Count(call));
            } else if (aggFunction.equals(SqlStdOperatorTable.SUM)) {
                aggFunctions.add(new SUM(call));
            } else if (aggFunction.equals(SqlStdOperatorTable.AVG)) {
                aggFunctions.add(new AVG(call));
            } else if (aggFunction.equals(SqlStdOperatorTable.MAX)) {
                aggFunctions.add(new MAX(call));
            } else if (aggFunction.equals(SqlStdOperatorTable.MIN)) {
                aggFunctions.add(new MIN(call));
            }
        }

    }

    @Override
    protected void doRun() {
        LOGGER.debug("run groupBy......"+logicalNode);
        // del old tuple
        for (Tuple tuple : res.values()) {
            sendToSinks(new Tuple(tuple, Tuple.State.DEL));
        }

        Tuple tuple;
        while ((tuple = this.source.pollFirst()) != null) {
            GroupKey key = new GroupKey(tuple);

            Tuple aggTuple = res.get(key);
            if (aggTuple == null) {
                Tuple model = new Tuple(new Object[getRowType().getFieldCount()], getRowType());
                int index = 0;
                for (int i : groupSet) {
                    model.set(index++, tuple.get(i));
                }
                res.put(key, model);
                aggTuple = model;
            }

            aggTuple.setTs(tuple.getTs());

            int currentPos = groupSet.cardinality();
            for (BaseAggFunction aggFunction : aggFunctions) {
                aggTuple.set(currentPos, aggFunction.eval(key, tuple));
                currentPos++;
            }
        }

        for (Tuple t : res.values()) {
           sendToSinks(new Tuple(t, Tuple.State.ADD));
        }
    }


    private abstract static class BaseAggFunction {
        protected int arg;
        protected AggregateCall call;

        public BaseAggFunction(AggregateCall call) {
            if (!call.getArgList().isEmpty()) {
                this.arg = call.getArgList().get(0);
                this.call = call;
            }
        }

        abstract Object eval(GroupKey key, Tuple tuple);
    }

    private static class Count extends BaseAggFunction {
        HashMap<GroupKey, Integer> synopsis = new HashMap<>();

        public Count(AggregateCall call) {
            super(call);
        }

        @Override
        public Object eval(GroupKey key, Tuple tuple) {
            if (!synopsis.containsKey(key)) {
                synopsis.put(key, 0);
            }
            int count = synopsis.get(key);
            if (tuple.getState().equals(Tuple.State.ADD)) {
                count++;
            } else {
                count--;
            }
            synopsis.put(key, count);
            return count;
        }
    }

    private static class SUM extends BaseAggFunction {
        HashMap<GroupKey, Object> synopsis = new HashMap<>();

        public SUM(AggregateCall call) {
            super(call);
        }

        @Override
        Object eval(GroupKey key, Tuple tuple) {
            String type = call.getType().getSqlTypeName().getName();
            Object n = tuple.get(arg);
            Object agg = synopsis.get(key);
            Object result = null;
            boolean add = tuple.getState() == Tuple.State.ADD;

            switch (type) {
                case "INTEGER":
                    if (add) {
                        result = agg == null ? n : (int) n + (int) agg;
                    } else {
                        result = (int) agg - (int) n;
                    }
                    break;
                case "DOUBLE":
                    if (add) {
                        result = agg == null ? n : (double) n + (double) agg;
                    } else {
                        result = (double) agg - (double) n;
                    }
                    break;
                case "BIGINT":
                    if (add) {
                        result = agg == null ? n : (long) n + (long) agg;
                    } else {
                        result = (long) agg - (long) n;
                    }
                    break;
                default:
                    break;
            }
            synopsis.put(key, result);

            return result;
        }
    }

    private static class AVG extends BaseAggFunction {

        private Count count;
        private SUM sum;

        public AVG(AggregateCall call) {
            super(call);
            this.count = new Count(call);
            this.sum = new SUM(call);
        }

        @Override
        Object eval(GroupKey key, Tuple tuple) {
            Integer c = (Integer) this.count.eval(key, tuple);
            Object s = this.sum.eval(key, tuple);
            String type = call.getType().getSqlTypeName().getName();
            BigDecimal result = null;
            switch (type) {
                case "INTEGER":
                    result = BigDecimal.valueOf(((Integer) s).doubleValue() / c).setScale(2, RoundingMode.CEILING);
                    break;
                case "BIGINT":
                    result = BigDecimal.valueOf(((Long) s).doubleValue() / c).setScale(2, RoundingMode.CEILING);
                    break;
                case "DOUBLE":
                    result = BigDecimal.valueOf((Double) s / c).setScale(2, RoundingMode.CEILING);
                    break;
                default:
                    break;
            }
            return result;
        }
    }

    private static class MAX extends BaseAggFunction {
        private HashMap<GroupKey, PriorityQueue<Comparable>> synopsis = new HashMap<>();

        public MAX(AggregateCall call) {
            super(call);
        }

        @Override
        Object eval(GroupKey key, Tuple tuple) {
            Object v = tuple.get(arg);

            PriorityQueue<Comparable> queue = synopsis.get(key);
            if (queue == null) {
                queue = new PriorityQueue<>((o1, o2) -> -o1.compareTo(o2));
                synopsis.put(key, queue);
            }
            if (tuple.getState().equals(Tuple.State.ADD)) {
                queue.add((Comparable) v);
            } else {
                queue.remove(v);
            }

            return queue.peek();
        }
    }

    private static class MIN extends BaseAggFunction {
        private HashMap<GroupKey, PriorityQueue<Comparable>> synopsis = new HashMap<>();

        public MIN(AggregateCall call) {
            super(call);
        }

        @Override
        Object eval(GroupKey key, Tuple tuple) {
            Object v = tuple.get(arg);
            PriorityQueue<Comparable> queue = synopsis.get(key);
            if (queue == null){
                queue = new PriorityQueue<>();
                synopsis.put(key,queue);
            }
            if (tuple.getState().equals(Tuple.State.ADD)) {
                queue.add((Comparable) v);
            } else {
                queue.remove(v);
            }
            return queue.peek();
        }
    }

    private class GroupKey {
        public GroupKey(Tuple tuple) {
            this.keys = new Object[groupSet.cardinality()];
            int index = 0;
            for (int i : groupSet) {
                keys[index++] = tuple.get(i);
            }
        }

        private Object[] keys;

        @Override
        public int hashCode() {
            return Arrays.hashCode(keys);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }

            if (obj instanceof GroupKey) {
                GroupKey o = (GroupKey) obj;
                if (Arrays.equals(this.keys, o.keys)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("keys: ");
            for (Object key : keys) {
                sb.append(key);
                sb.append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            return sb.toString();
        }
    }
}
