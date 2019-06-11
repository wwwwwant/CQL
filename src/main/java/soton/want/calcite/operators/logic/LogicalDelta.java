package soton.want.calcite.operators.logic;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.stream.Delta;
import org.apache.calcite.rex.RexNode;

/**
 * LogicalNode which transform a Relation to a Stream
 * @author want
 */
public class LogicalDelta extends Delta {

    private RexNode condition;

    protected LogicalDelta(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
    }

    public LogicalDelta(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode condition) {
        this(cluster,traitSet,input);
        this.condition = condition;
    }

    public static LogicalDelta create(RelNode input, RexNode condition){
        return new LogicalDelta(input.getCluster(),input.getTraitSet(),input, condition);
    }

    public RexNode getCondition() {
        return condition;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("Delta",condition);
    }
}
