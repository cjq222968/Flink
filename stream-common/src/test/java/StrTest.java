/**
 * @BelongsProject: dev-test
 * @BelongsPackage: PACKAGE_NAME
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-19  14:09
 * @Description: TODO
 * @Version: 1.0
 */
public class StrTest {
    private static int[] staticArray = new int[5];
    // 用于记录当前数组中已经存放的元素个数
    private static int size = 0;
    public static void main(String[] args) {

    }
    void add(int e ){
        if (size == staticArray.length) {
            // 简单扩容为原来的2倍，这里也可以采用更复杂合理的扩容策略
            int[] newArray = new int[staticArray.length * 2];
            // 将原数组元素复制到新数组
            System.arraycopy(staticArray, 0, newArray, 0, staticArray.length);
            staticArray = newArray;
        }
        // 将元素添加到数组中
        staticArray[size] = e;
        size++;
    }
}
