package com.ringcentral.analytics.template;

import com.ringcentral.analytics.test.oozie.OozieTestHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * @author konstantin.lysunkin
 * 4/24/19
 */
@RunWith(Parameterized.class)
public class OozieTest {

    @Parameterized.Parameter(0)
    public String wfName;

    @Parameterized.Parameter(1)
    public boolean withCoord;

    @Parameterized.Parameters(name = "{index}: Validating {0} ")
    public static Object[][] data() {
        return new Object[][]{
                {"workflow-template", true}};
    }

    @Test
    public void validateWFDefinition() throws Exception {
        OozieTestHelper helper = new OozieTestHelper();

        String wfResourceName = "workflows/" + wfName + "/workflow.xml";
        helper.validateResource(wfResourceName);

        if (withCoord) {
            String coordResourceName = "workflows/" + wfName + "/coordinator.xml";
            helper.validateResource(coordResourceName);
        }
    }
}
