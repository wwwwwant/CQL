package soton.want.calcite.operators.physic;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.NlsString;
import soton.want.calcite.operators.Tuple;
import soton.want.calcite.operators.Utils;
import soton.want.calcite.operators.logic.LogicalDelta;

/**
 * @author want
 */
public class DeltaOperator extends UnaryOperator<LogicalDelta> {

    private RexNode condition;
    private String type;

    public DeltaOperator(LogicalDelta logicalNode, Operator child) {
        super(logicalNode, child);
        this.condition = logicalNode.getCondition();
        this.type = ((NlsString)Eval.eval(condition,null)).getValue();
    }

    @Override
    public void run() {

        Tuple tuple = source.pollFirst();
        if (tuple == null){
            return;
        }
        Utils.printTypes(tuple);

        while (tuple!=null){
            if (type.equals(tuple.getState().toString()) || type.equals("RET")){
                Utils.printTuple(tuple);
            }
            tuple = source.pollFirst();
        }


    }
}
