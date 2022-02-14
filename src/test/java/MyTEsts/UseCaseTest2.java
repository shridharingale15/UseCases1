package MyTEsts;
import My_UsedCases.UsedCase2;
import org.junit.Assert;
import org.junit.Test;

public class UseCaseTest2 {
    @Test
    public void My_func3(){
        Assert.assertEquals(0, UsedCase2.My_func_swap());
    }
}