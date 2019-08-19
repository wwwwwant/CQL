package soton.want.calcite.plan;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.*;

/**
 * @author want
 */
public class LinearRoadFactory implements TableFactory<Table> {

    @Override
    public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
        CarFactory carFactory;
//        carFactory = new SameSpeedCarFactory();
        carFactory = new FastSlowCarFactory();

        return new InfinitePosStream(carFactory);
    }


    private abstract static class BaseStreamTable implements ScannableTable {
        protected final RelProtoDataType protoRowType = a0 -> a0.builder()
                .add("vehicleId", SqlTypeName.INTEGER)
                .add("speed", SqlTypeName.DOUBLE)
                .add("xPos", SqlTypeName.DOUBLE)
                .build();

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return protoRowType.apply(typeFactory);
        }

        @Override
        public Statistic getStatistic() {
            return Statistics.of(100d, ImmutableList.of(),
                    RelCollations.createSingleton(0));
        }

        @Override
        public Schema.TableType getJdbcTableType() {
            return Schema.TableType.TABLE;
        }

        @Override
        public boolean isRolledUp(String column) {
            return false;
        }

        @Override
        public boolean rolledUpColumnValidInsideAgg(String column,
                                                    SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
            return false;
        }
    }

    /**
     * Table representing an infinitely larger ORDERS stream.
     */
    public static class InfinitePosStream extends BaseStreamTable
            implements StreamableTable {
        private CarFactory carFactory;

        public InfinitePosStream(CarFactory carFactory) {
            this.carFactory = carFactory;
        }

        @Override
        public Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(() -> new Iterator<Object[]>() {
                List<Object[]> carsReport = carFactory.getCarsReport();
                Iterator<Object[]> iterator = carsReport.iterator();

                @Override
                public boolean hasNext() {
                    if (iterator == null || !iterator.hasNext()) {
                        carsReport = carFactory.getCarsReport();
                        iterator = carsReport.iterator();
                        try {
                            // 5s 发送一次位置报告
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    return true;
                }

                @Override
                public Object[] next() {
                    return iterator.next();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            });
        }

        @Override
        public Table stream() {
            return this;
        }






    }
}

