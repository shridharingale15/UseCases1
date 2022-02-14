package MyTEsts;
import My_UsedCases.UsedCase4;
import org.junit.Assert;
import org.junit.Test;

public class UseCaseTest4 {
    @Test
    public void My_func5(){
        Assert.assertEquals(33, UsedCase4.My_func_swap2());
    }
}