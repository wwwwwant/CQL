package soton.want.calcite.operators.physic;

import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import soton.want.calcite.operators.Tuple;

/**
 * @author want
 */
public class FilterOperator extends UnaryOperator<Filter> {

    private RexNode condition;

    public FilterOperator(Filter logicalNode, Operator child) {
        super(logicalNode, child);
        this.condition = logicalNode.getCondition();
    }

    @Override
    public void run() {
        Tuple tuple = null;
        while ((tuple=source.pollFirst())!=null){
            Boolean comp = (Boolean) Eval.eval(condition,tuple);
            if (comp!=null && comp){
                this.sink.addLast(tuple);
            }
        }

        runParent();
    }
}
