package com.ringcentral.analytics.template;

import com.ringcentral.analytics.test.deployment.DeploymentTestHelper;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Map;

/**
 * @author konstantin.lysunkin
 */
@RunWith(Parameterized.class)
public class DeploymentTest {

    private static DeploymentTestHelper helper;

    @Parameterized.Parameter
    public String resource;

    @Parameterized.Parameter(1)
    public boolean render;

    @Parameterized.Parameters(name = "{index}: Validating {0} ")
    public static Object[][] data() {
        return new Object[][]{
                {"inventory.yaml.j2", true},
                {"internal_vars.yaml.j2", true},
                {"external_vars.yaml", true},
                {"scenarios/clean_data.yaml", true},
                {"scenarios/deploy.yaml", true},
                {"scenarios/deploy_oozie_coordinators.yaml", true},
                {"scenarios/start.yaml", true},
                {"scenarios/stop.yaml", true},
                {"scenarios/undeploy.yaml", true},
                {"include/deploy_oozie_coordinators.yaml", true}
        };
    }

    @BeforeClass
    @SuppressWarnings("unchecked")
    public static void setUp() throws IOException {
        helper = new DeploymentTestHelper();

        Map<String, Object> allVars = helper.getAllVars();
        Map<String, Object> common = (Map<String, Object>) allVars.get("common");
        Map<String, Object> production = (Map<String, Object>) allVars.get("production");
        allVars.putAll(common);
        allVars.putAll(production);
    }

    @Test
    public void testValidity() throws IOException {

        Object result = render ? helper.renderAndConvertResource(resource) : helper.convertResource(resource);

        Assert.assertNotNull(result);
    }
}
