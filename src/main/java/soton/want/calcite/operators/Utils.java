package soton.want.calcite.operators;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import soton.want.calcite.operators.logic.LogicalDelta;
import soton.want.calcite.operators.logic.LogicalTupleWindow;
import soton.want.calcite.operators.physic.Operator;
import soton.want.calcite.plan.SimpleQueryPlanner;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author want
 */
public class Utils {


//    public static RelOptTable getRelOptTable(){
//        MemoryData.Table student = MemoryData.MAP.get("school").tables.get(0);
//
//        MemoryTable memoryTable = new MemoryTable(student);
//
//        RelDataType rowType = memoryTable.getRowType(new JavaTypeFactoryImpl());
//
//        ImmutableList<String> names = ImmutableList.of("school","Student");
//
//        RelOptTableImpl relOptTable = RelOptTableImpl.create(null, rowType, memoryTable, names);
//
//        return relOptTable;
//    }


    public static RelBuilder getRelBuilder(String model) {

        try {
            return SimpleQueryPlanner.supplyRelBuilder(model);
        }catch (Exception e){
            System.out.println(e);
        }

        return null;
    }

    public static void printTypes(Tuple tuple){
        StringBuilder sb = new StringBuilder();
        List<RelDataTypeField> fieldList = tuple.getRowType().getFieldList();
        for (RelDataTypeField field : fieldList){
            sb.append(field.getName()).append(",");
        }
        sb.deleteCharAt(sb.length()-1);
        System.out.println(sb.toString());


        sb = new StringBuilder();
        for (RelDataTypeField field : fieldList){
            sb.append(field.getType()).append(",");
        }
        sb.deleteCharAt(sb.length()-1);
        System.out.println(sb.toString());
    }


    public static void printTuple(Tuple tuple){
        StringBuilder sb = new StringBuilder();
        Object[] row = tuple.getValues();

        for (Object obj : row){
            sb.append(obj).append(",");
        }
        sb.append(tuple.getState());
        System.out.println(sb.toString());
    }



}
