package soton.want.calcite.operators.physic;

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.log4j.Logger;
import soton.want.calcite.operators.Tuple;
import soton.want.calcite.operators.TupleQueue;

import java.util.Iterator;

/**
 * just as described in standford Stream
 * maintain two synopsis, run join when tuple is added or deleted from the synopsis
 * @author want
 */
public class JoinOperator extends BiOperator<Join> {

    private static final Logger LOGGER = Logger.getLogger(JoinOperator.class);

    private Join relJoin;

    private RexNode condition;
    private final JoinRelType joinType;

    private TupleQueue leftSynopsis;
    private TupleQueue rightSynopsis;

    public JoinOperator(Join logicalNode, Operator left, Operator right) {
        super(logicalNode, (AbstractOperator) left, (AbstractOperator) right);
        leftSynopsis = new TupleQueue();
        rightSynopsis = new TupleQueue();
        this.relJoin = logicalNode;
        this.condition = logicalNode.getCondition();
        this.joinType = logicalNode.getJoinType();
    }

    @Override
    public void doRun() {
        LOGGER.debug("run join......"+logicalNode);
        runSource(leftSource);
        runSource(rightSource);
    }

    /**
     * consume tuple in source, run join, and add the join result to the sink.
     * @param source sink of a child
     */
    private void runSource(TupleQueue source){
        TupleQueue joinSynopsis = rightSynopsis, synopsis = leftSynopsis;
        boolean isLeft = true;
        if (source==rightSource){
            joinSynopsis = leftSynopsis;
            synopsis = rightSynopsis;
            isLeft = false;
        }

        Tuple tuple = null;
        while ((tuple=source.pollFirst())!=null){
            runJoin(tuple,joinSynopsis,isLeft);
            if (tuple.getState()==Tuple.State.ADD){
                synopsis.addLast(tuple);
            }else{
                Iterator<Tuple> iterator = synopsis.iterator();
                while (iterator.hasNext()){
                    Tuple t = iterator.next();
                    if (t.equals(tuple)){
                        iterator.remove();
                        break;
                    }
                }
            }
        }
    }

    private void runJoin(Tuple tuple, TupleQueue synopsis, boolean isLeftTuple){
        Tuple.State state = tuple.getState();
        Object[] v1 = null;
        Object[] v2 = null;
        Tuple res= null;
        Iterator<Tuple> iterator = synopsis.iterator();
        int leftCount = relJoin.getLeft().getRowType().getFieldCount();
        int rightCount = relJoin.getRight().getRowType().getFieldCount();

        if (isLeftTuple){
            v1 = tuple.getValues();

            while (iterator.hasNext()){
                Object[] val = new Object[relJoin.getRowType().getFieldCount()];

                System.arraycopy(v1,0,val,0,leftCount);

                Tuple right = iterator.next();
                v2 = right.getValues();

                System.arraycopy(v2,0,val,leftCount,rightCount);

                res = new Tuple(val,relJoin.getRowType(),state,tuple.getTs());
                Boolean isJoin = (Boolean) Eval.eval(condition,res);
                if (isJoin != null && isJoin){
                    sendToSinks(res);
                }
            }
        }else{
            v2 = tuple.getValues();


            while (iterator.hasNext()){
                Object[] val = new Object[relJoin.getRowType().getFieldCount()];

                System.arraycopy(v2,0,val,leftCount,rightCount);

                Tuple left = iterator.next();
                v1 = left.getValues();

                System.arraycopy(v1,0,val,0,leftCount);

                res = new Tuple(val,relJoin.getRowType(),state,tuple.getTs());
                Boolean isJoin = (Boolean) Eval.eval(condition,res);
                if (isJoin != null && isJoin){
                    sendToSinks(res);
                }
            }
        }





    }
}
