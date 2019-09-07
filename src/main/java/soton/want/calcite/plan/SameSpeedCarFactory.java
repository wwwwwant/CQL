package soton.want.calcite.plan;

import java.util.ArrayList;
import java.util.List;

/**
 * @author want
 * @Date 30/07/2019
 */
public class SameSpeedCarFactory implements CarFactory {

    Car[] cars;
    int id = 1;
    double roadLength = 1000;
    int interval = 5;

    /**
     * all cars in every segment would be charged
     */
//    int numCars = 2;
//    double speed = 10;

    /**
     * no car would be charged
     */
    int numCars = 2;
    double speed = 35;


    List<Object[]> report = new ArrayList<>();

    {
        cars = new Car[numCars];
        for (int i = 0; i < cars.length; i++) {
            cars[i] = new Car(id++, speed, 0);
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
