package soton.want.calcite.operators;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.TimeString;
import soton.want.calcite.plan.SimpleQueryPlanner;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
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

    static final DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String formatDate(long ts){
        return format.format(new Date(ts));
    }


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
        sb.deleteCharAt(sb.length()-1);
//        sb.append(tuple.getState());
//        sb.append(",");

//        sb.append(formatDate(tuple.getTs()));

        System.out.println(sb.toString());
    }

    public static RexNode createTimeInterval(RelBuilder builder,int h, int m, int s){
        RelDataTypeFactory typeFactory = builder.getTypeFactory();
        RexBuilder rexBuilder = builder.getRexBuilder();
        RelDataType timeType = typeFactory.createSqlType(SqlTypeName.TIME);

        final TimeString t = new TimeString(h, m, s);
        return rexBuilder.makeLiteral(t,timeType,false);

    }



}
