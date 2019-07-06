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
        return new InfinitePosStream();
    }


    private abstract static class BaseStreamTable implements ScannableTable {
        protected final RelProtoDataType protoRowType = a0 -> a0.builder()
                .add("vehicleId", SqlTypeName.INTEGER)
                .add("speed", SqlTypeName.DOUBLE)
                .add("xPos",SqlTypeName.DOUBLE)
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

        @Override public boolean isRolledUp(String column) {
            return false;
        }

        @Override public boolean rolledUpColumnValidInsideAgg(String column,
                                                              SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
            return false;
        }
    }

    /**
     * Table representing an infinitely larger ORDERS stream.
     */
    public static class InfinitePosStream extends BaseStreamTable
            implements StreamableTable {

        @Override
        public Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(() -> new Iterator<Object[]>() {
                List<Object[]> carsReport = CarFactory.getCarsReport();
                Iterator<Object[]> iterator = carsReport.iterator();
                @Override
                public boolean hasNext() {
                    if (iterator ==null || !iterator.hasNext()){
                        carsReport = CarFactory.getCarsReport();
                        iterator = carsReport.iterator();
                        try {
                            Thread.sleep(30000);
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


        /**
         * road length: 1000
         */
        static class Car{
            int vehicleId;
            double speed;
            double xPos;

            public Car(int id,double speed, double pos){
                this.vehicleId = id;
                this.speed = speed;
                this.xPos = pos;
            }

            public double getCurPos(){
                return this.xPos;
            }

            public void setVehicleId(int vehicleId) {
                this.vehicleId = vehicleId;
            }

            public void setxPos(double xPos) {
                this.xPos = xPos;
            }

            public Object[] report() {
                Object[] res = new Object[]{vehicleId,speed,xPos};
                this.xPos += speed*30/3600;
                return res;
            }
        }


        static class CarFactory{
            static Car[] cars;
            static int id = 1;
            static double speed = 30;
            static double roadLength = 1000;
            static List<Object[]> report = new ArrayList<>();

            static {
                cars = new Car[10];
                for (int i=0;i<cars.length;i++){
                    cars[i] = new Car(id++,speed,0);
                    report.add(cars[i].report());
                }
            }

            static List<Object[]> getCarsReport(){
                for (int i=0;i<cars.length;i++){
                    Car car = cars[i];
                    if (car.getCurPos()>roadLength){
                        car.setVehicleId(id++);
                        car.setxPos(0);
                    }
                    report.set(i,car.report());
                }
                return report;
            }
        }

    }
}

