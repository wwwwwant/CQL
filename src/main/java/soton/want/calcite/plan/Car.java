package soton.want.calcite.plan;

/**
 * @author want
 * @Date 30/07/2019
 */
public class Car {
    int vehicleId;
    double speed;
    double xPos;

    public Car(int id, double speed, double pos) {
        this.vehicleId = id;
        this.speed = speed;
        this.xPos = pos;
    }

    public double getCurPos() {
        return this.xPos;
    }

    public void setVehicleId(int vehicleId) {
        this.vehicleId = vehicleId;
    }

    public void setxPos(double xPos) {
        this.xPos = xPos;
    }

    // return the report
    public Object[] report(int seconds) {
        Object[] report = new Object[]{vehicleId, speed, xPos};
        this.xPos += speed * seconds;

        return report;
    }
}
