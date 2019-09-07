import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.log4j.Logger;
import soton.want.calcite.operators.Context;
import soton.want.calcite.operators.RelToOperators;
import soton.want.calcite.operators.Utils;
import soton.want.calcite.operators.logic.LogicalDelta;
import soton.want.calcite.operators.logic.LogicalRStream;
import soton.want.calcite.operators.logic.LogicalWindow;
import soton.want.calcite.operators.physic.Operator;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static soton.want.calcite.operators.Utils.getRelBuilder;

/**
 * @author want
 */
public class TestCQL {
    /**
     * RelBuilder is used to build logical plan with api and schema
     * no need to parse SQL
     */
    private static final Logger LOGGER = Logger.getLogger(TestCQL.class);
    private static RelBuilder builder = getRelBuilder("schema.json");
    private static Context context = Context.getInstance();


    public static void main(String[] args) {

        /**
         * stream schema: Orders(id, userId, product, units)
         */



        /**
         * test time-based window and I/D/RStream operators
         *
         * SELECT I/D/RStream *
         * FROM Orders[Range 00:00:10]
         */
//        timeWindowToStream("ADD");
//        timeWindowToStream("DEL");
//        timeWindowToStream("RStream");

        /**
         * test project and filter operators
         *
         * SELECT RStream id, productName
         * FROM Orders[Range 00:00:10]
         * WHERE ID>10;
         */
//        timeWinProjectFilter();

        /**
         * test groupBy operator (units are random int between 1 and 10)
         *
         * SELECT RStream userId, productName, SUM(units), COUNT(1), AVG(units), MAX(units), MIN(units)
         * FROM Orders[Range 00:00:10]
         * GROUP BY userId,product
         */
//        timeWindowgroupBy();

        /**
         * test join and groupBy
         * relation schema: User(userId, name)
         *
         * SELECT RStream userId, name, SUM(units) as sum, count(*) as count
         * FROM Orders[00:00:10] as o join User as u
         * ON o.userId=u.userId
         * GROUP BY userId,name
         */
        streamJoinGroupBy();


    }

    private static void timeWindowToStream(String type) {

        RelNode t1 = builder.scan("Orders").build();
        RexNode timeInterval = Utils.createTimeInterval(builder, 0, 0, 10);
        LogicalWindow timeWindow = LogicalWindow.create(t1, timeInterval);

        RelNode head = null;
        if (type.equals("ADD") || type.equals("DEL")){
                head = LogicalDelta.create(timeWindow,builder.literal(type));
        }else {
            head = LogicalRStream.create(timeWindow);
        }

        context.transformAndRun(head);

    }

    private static void timeWinProjectFilter(){

        RelNode relScan = builder.scan("Orders").build();
        RexNode timeInterval = Utils.createTimeInterval(builder, 0, 0, 10);

        LogicalWindow relWindow = LogicalWindow.create(relScan, timeInterval);
        RelNode relProject = builder
                .push(relWindow)
                .filter(builder.call(SqlStdOperatorTable.GREATER_THAN,builder.field("ID"),builder.literal(10)))
                .project(builder.field("ID")
                        ,builder.field("PRODUCTNAME")).build();

        LogicalRStream rStream = LogicalRStream.create(relProject);

        context.transformAndRun(rStream);

    }


    private static void timeWindowgroupBy(){
        RelNode t1 = builder.scan("Orders").build();
        RexNode timeInterval = Utils.createTimeInterval(builder, 0, 0, 10);
        LogicalWindow window1 = LogicalWindow.create(t1, timeInterval);

        builder.push(window1)
                .aggregate(builder.groupKey("USERID","PRODUCTNAME"),
                        builder.sum(false, "S", builder.field("UNITS")),
                        builder.count(false, "C"),
                        builder.avg(false,"AVG",builder.field("UNITS")),
                        builder.max("MAX",builder.field("UNITS")),
                        builder.min("MIN",builder.field("UNITS"))
                );

        RelNode node = builder.build();
        LogicalRStream rStream = LogicalRStream.create(node);

        System.out.println(RelOptUtil.toString(rStream));

        context.transformAndRun(rStream);

    }


    public static void streamJoinGroupBy(){

        RelNode t1 = builder.scan("Orders").build();

        RexNode timeInterval = Utils.createTimeInterval(builder, 0, 0, 10);
        LogicalWindow window1 = LogicalWindow.create(t1, timeInterval);

        RelNode t2 = builder.scan("User").build();

        builder.push(window1);
        builder.push(t2);

        builder.join(JoinRelType.INNER,"USERID");

        List<RexNode> fields = new ArrayList<>();
        String[] fieldNames = new String[]{"USERID","PRODUCTNAME","NAME","USERID","UNITS"};

        for (String id: fieldNames){
            fields.add(builder.field(id));
        }

        builder.project(fields)
                .aggregate(builder.groupKey("USERID","PRODUCTNAME","NAME"),
                builder.sum(false, "SUM", builder.field("UNITS")),
                builder.count(false, "COUNT"));

        RelNode build = builder.build();
        LogicalRStream rStream = LogicalRStream.create(build);

        System.out.println(RelOptUtil.toString(rStream));

        context.transformAndRun(rStream);
    }




}
