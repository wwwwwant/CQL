import java.math.BigDecimal;
import java.math.MathContext;

/**
 * @author want
 */
public class TestBigDecimal {
    public static void main(String[] args) {
        BigDecimal b1 = new BigDecimal(228.2);
        BigDecimal b2 = new BigDecimal(230);

        System.out.println(b1.divide(b2, new MathContext(10)).longValue());
    }
}
