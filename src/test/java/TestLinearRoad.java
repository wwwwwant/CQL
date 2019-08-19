import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.log4j.Logger;
import soton.want.calcite.operators.Context;
import soton.want.calcite.operators.RelToOperators;
import soton.want.calcite.operators.Utils;
import soton.want.calcite.operators.logic.LogicalDelta;
import soton.want.calcite.operators.logic.LogicalRStream;
import soton.want.calcite.operators.logic.LogicalWindow;
import soton.want.calcite.operators.physic.Operator;

import java.util.ArrayList;
import java.util.List;

import static soton.want.calcite.operators.Utils.getRelBuilder;

/**
 * @author want
 */
public class TestLinearRoad {
    private static RelBuilder builder = getRelBuilder("schema.json");
    private static final Context CONTEXT = Context.getInstance();
    private static final Logger LOGGER = Logger.getLogger(TestLinearRoad.class);

    public static void main(String[] args) {

        /**
         * SegSpeedStr(vehicleId, speed, segNo)
         *
         * SELECT vehicleId, speed,
         * xPos/100 as segNo
         * From PosSpeedStr
         */
        RelNode table = builder.scan("LinearRoad").build();

        LogicalWindow unboundedWin = LogicalWindow.create(table,builder.literal(-1));

        builder.push(unboundedWin);

        RexNode xPos = builder.call(SqlStdOperatorTable.DIVIDE, builder.field("xPos"), builder.literal(100)) ;
        RexNode castSeg = builder.cast(xPos, SqlTypeName.BIGINT);
        RexNode segNo = builder.alias(castSeg,"segNo");

        RelNode segSpeedRel = builder.project(
                builder.field("vehicleId")
                , builder.field("speed")
                , segNo
        ).build();

        // SegSpeedStr
        LogicalDelta segSpeedStr = LogicalDelta.create(segSpeedRel, builder.literal("ADD"));


        /**
         * ActiveVehicleSegRel(vehicleId,segNo)
         * 5 seconds for the heartbeat
         *
         * 每一批有 del 也有 add
         */
        RexNode timeInterval = Utils.createTimeInterval(builder, 0, 0, 5);
        LogicalWindow activeTimeWin = LogicalWindow.create(segSpeedStr, timeInterval);

        RelNode activeVehicleSegRel = builder.push(activeTimeWin).project(
                builder.field("vehicleId")
                , builder.field("segNo")
        ).build();


        /**
         * VehicleSegEntryStr(vehicleId,segNo)
         * 永远是输出 add，每 5s 输出一次最新数据
         *
         * VehicleSegEntryRel 加了一个 now window
         * 每次先输出旧的 del，再输出新的 add
         */
        RexNode nowWin = Utils.createTimeInterval(builder, 0, 0, 0);
        LogicalDelta vehicleSegEntryStr = LogicalDelta.create(activeVehicleSegRel, builder.literal("ADD"));

        // used in TollStr
        LogicalWindow vehicleSegEntryRel = LogicalWindow.create(vehicleSegEntryStr, nowWin);


        /**
         * CongestedSegRel(segNo)
         * last 1 min avg speed to define the congested segment
         *
         * 由于 1 分钟时间长，在现在的设置下，所有 segment 都是 congested
         * 现在里面会缓存同一辆车的很多次 report
         * 每次接收新的 add，先输出 del 的 group 再聚合 add，最后输出聚合后的 add
         */
        RexNode timeWin_5 = Utils.createTimeInterval(builder, 0, 1, 0);

        RelNode congestedSegRel = builder.push(LogicalWindow.create(segSpeedStr, timeWin_5))
                .aggregate(
                        builder.groupKey(builder.field("segNo"))
                        , builder.avg(false, "avgSpeed", builder.field("speed"))
                )
                .filter(builder.call(SqlStdOperatorTable.LESS_THAN, builder.field("avgSpeed"), builder.literal(30)))
                .project(builder.field("segNo")).build();

        /**
         * SegVolRel(segNo,numVehicles)
         *
         * 输出的总是 add，每次先接收 del，清除上一批次 segNo 里的 state
         * 再接收新的 segNo 里的数据
         *
         * 现在的写法是，groupBy 会先输出 del 清除状态，再输出 add 输出新状态
         */

        RelNode segVolRel = builder.push(activeVehicleSegRel)
                .aggregate(
                        builder.groupKey("segNo")
                        , builder.count(false, "numVehicles")
                ).build();


        /**
         * TollStr(vehicleId,toll)
         */


        builder.push(segVolRel)
                .push(congestedSegRel)
                .join(JoinRelType.INNER, "segNo")
                .push(vehicleSegEntryRel)
                .join(JoinRelType.INNER, "segNo");

        RexNode substract = builder.call(SqlStdOperatorTable.MINUS,builder.field("numVehicles"),builder.literal(50));
        RexNode multiply = builder.call(SqlStdOperatorTable.MULTIPLY,substract,builder.literal(2));
        RexNode multiply2 = builder.call(SqlStdOperatorTable.MULTIPLY,multiply,substract);

        RexNode alias = builder.call(SqlStdOperatorTable.AS,multiply2,builder.literal("toll"));

        RelNode toll = builder.project(builder.field("vehicleId"), alias,builder.field("segNo")).build();

        LogicalRStream tollStr = LogicalRStream.create(toll);


        /**
         * test TollStr
         */

        System.out.println(RelOptUtil.toString(tollStr));

        RelToOperators visitor = new RelToOperators();

        Operator physicalPlan = visitor.visit(tollStr);

//        physicalPlan.explain(0);
        List<Operator> tables = visitor.getTables();

        CONTEXT.runOperator(tables);

    }




    public static void testOperator(List<Operator> tables){
        while (true){
            long startTs = System.currentTimeMillis();
            CONTEXT.setCurrentTs(startTs);
            LOGGER.info("windowEndTs: "+Utils.formatDate(startTs));
            for (Operator table : tables){
                table.run();
            }
            System.out.println("------------------------------------");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
