package soton.want.calcite.operators.physic;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import soton.want.calcite.operators.Tuple;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.List;


/**
 * This class evaluates expressions
 * <li>get a field from a tuple {@link RexInputRef}</li>
 * <li>get a literal value from {@link RexLiteral}</li>
 * <li>evaluate join condition, filter condition, field computation in {@link RexCall}</li>
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

            if (operands.size()==2){

                RexNode rex0 = operands.get(0);
                RexNode rex1 = operands.get(1);

                switch (kind) {
                    case AS:
                        return eval(rex0);
                    default:break;
                }


                BigDecimal b0,b1;

                b0 = getBigDecimal(eval(rex0,tuple));
                b1 = getBigDecimal(eval(rex1,tuple));

                MathContext mc = MathContext.DECIMAL32;


                switch (kind) {
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                    case EQUALS:
                    case NOT_EQUALS:
                        int c = b0.compareTo(b1);

                        switch (kind) {
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
                    case PLUS:
                        return b0.add(b1,mc);
                    case DIVIDE:
                        return b0.divide(b1,mc);
                    case MINUS:
                        return b0.subtract(b1,mc);
                    case TIMES:
                        return b0.multiply(b1,mc);

                    default:break;
                }
            }else if (operands.size()==1){
                switch (kind) {
                    case CAST:
                        SqlTypeName typeName = call.getType().getSqlTypeName();
                        switch (typeName){
                            case BIGINT:
                            case INTEGER:
                                BigDecimal eval = (BigDecimal) eval(operands.get(0),tuple);
                                return eval.longValue();

                            default:break;
                        }

                    default:break;
                }
            }




        }

        return null;
    }

    private static BigDecimal getBigDecimal(Object obj){
        if (obj instanceof BigDecimal){
            return (BigDecimal) obj;
        }else if (obj instanceof Double || obj instanceof Float){
            return new BigDecimal(((Number)obj).doubleValue());
        }else if (obj instanceof Integer || obj instanceof Long){
            return new BigDecimal(((Number)obj).longValue());
        }else {
            return null;
        }
    }

}
