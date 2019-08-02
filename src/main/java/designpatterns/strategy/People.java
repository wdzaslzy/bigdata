package designpatterns.strategy;

/**
 * @author zhongyou_li
 */
public abstract class People {

    protected DisplayInterface displayInterface;

    public void display() {
        displayInterface.display();
    }

    public abstract void country();

    public abstract void say();

    public void common() {
        System.out.println("一个头，两个手");
    }

    public void setDisplayInterface(DisplayInterface displayInterface) {
        this.displayInterface = displayInterface;
    }
}
