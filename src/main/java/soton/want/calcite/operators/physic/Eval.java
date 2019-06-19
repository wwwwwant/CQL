package soton.want.calcite.operators.physic;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import soton.want.calcite.operators.Tuple;

import java.math.BigDecimal;
import java.util.List;


/**
 * This class evaluates expressions
 * <li>get a field from a tuple {@link RexInputRef}</li>
 * <li>get a literal value from {@link RexLiteral}</li>
 * <li>evaluate join condition, filter condition in {@link RexCall}</li>
 *
 * @author want
 */
public class Eval {

    static Object eval(RexNode rex){
        return eval(rex,null);
    }

    static Object eval(RexNode rex, Tuple tuple){

        // RexInputRef represent a field in a tuple
        if (rex instanceof RexInputRef){
            int index = ((RexInputRef) rex).getIndex();
            return (Comparable)tuple.getField(index);

        } else if (rex instanceof RexLiteral){
            RexLiteral literal = (RexLiteral) rex;
            SqlTypeName typeName = literal.getTypeName();
//            switch (typeName){
//                case DECIMAL: return literal.getValue();
//                case CHAR: return literal.getValue();
//                default: return null;
//            }
            return literal.getValue2();

        }else if (rex instanceof RexCall){
            // used in filter condition, join condition,
            RexCall call = (RexCall) rex;
            SqlKind kind = call.getKind();
            List<RexNode> operands = call.getOperands();

            RexNode rex0 = operands.get(0);
            RexNode rex1 = operands.get(1);

            Comparable o0,o1;
            if (rex0 instanceof RexInputRef){
                o0 = (Comparable) eval(rex0,tuple);
                o1 = (Comparable) eval(rex1,tuple);
            }else{
                o0 = (Comparable) eval(rex1,tuple);
                o1 = (Comparable) eval(rex0,tuple);
            }

            if (o0 instanceof Number) {
                if (o1 instanceof Double || o1 instanceof Float) {
                    o1 = new BigDecimal(((Number) o1).doubleValue());
                } else {
                    o1 = new BigDecimal(((Number) o1).longValue());
                }
            }
            if (o1 instanceof Number) {
                if (o0 instanceof Double || o0 instanceof Float) {
                    o0 = new BigDecimal(((Number) o0).doubleValue());
                } else {
                    o0 = new BigDecimal(((Number) o0).longValue());
                }
            }

            int c = o0.compareTo(o1);

            switch (kind){
                case LESS_THAN:
                    return c < 0;
                case LESS_THAN_OR_EQUAL:
                    return c <= 0;
                case GREATER_THAN:
                    return c > 0;
                case GREATER_THAN_OR_EQUAL:
                    return c >= 0;
                case EQUALS:
                    return c == 0;
                case NOT_EQUALS:
                    return c != 0;
                default:
                    throw new AssertionError("unknown expression " + call);
            }

        }

        return null;
    }

}
