package MyTEsts;

import My_UsedCases.UsedCase1;
import org.junit.Assert;
import org.junit.Test;

public class UseCaseTest1 {
    @Test
    public void My_func2(){
        Assert.assertEquals(4696, UsedCase1.My_func());
    }
}