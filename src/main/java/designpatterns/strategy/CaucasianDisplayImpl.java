package designpatterns.strategy;

/**
 * @author zhongyou_li
 */
public class CaucasianDisplayImpl implements DisplayInterface {

    @Override
    public void display() {
        System.out.println("白种人");
    }
}
