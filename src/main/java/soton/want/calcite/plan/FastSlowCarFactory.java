package soton.want.calcite.plan;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xingkui
 * @Date 31/07/2019
 */
public class FastSlowCarFactory implements CarFactory{

    /**
     * road length: 1000m
     * each segment 100m
     * report will be sent every 5 seconds
     * congested segment is defined based on last 1 min reports (speed less than 30m/s)
     * the speed of car 20~50m/s
     */

    private int numCars = 2;
    private Car[] cars = new Car[numCars];

    private double[] carSpeeds = new double[]{10,35};
//    private double[] carSpeeds = new double[]{10,60};

    private int id = 1;
    private double roadLength = 1000;
    private int interval = 5;


    private List<Object[]> report = new ArrayList<>();

    {
        for (int i = 0; i < cars.length; i++) {
            cars[i] = new Car(id++, carSpeeds[i], 0);
            report.add(cars[i].report(interval));
        }
    }


    @Override
    public List<Object[]> getCarsReport() {
        for (int i = 0; i < cars.length; i++) {
            Car car = cars[i];
            if (car.getCurPos() > roadLength) {
                car.setVehicleId(id++);
                car.setxPos(0);
            }
            report.set(i, car.report(interval));
        }
        return report;
    }
}



