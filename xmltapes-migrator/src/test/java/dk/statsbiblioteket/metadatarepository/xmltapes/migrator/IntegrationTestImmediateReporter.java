package dk.statsbiblioteket.metadatarepository.xmltapes.migrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

/**
 * Created by abr on 08-12-15.
 */
public class IntegrationTestImmediateReporter implements ITestListener {

    private static final Logger log = LoggerFactory.getLogger(IntegrationTestImmediateReporter.class);

    @Override
    public void onTestStart(ITestResult result) {
        //extra spaces to align output
        System.out.println("Running test:      " + result.getTestClass().getName() + "#" + result.getName());

    }

    @Override
    public void onTestSuccess(ITestResult result) {
        System.out.println("Ending in Success: " + result.getTestClass().getName() + "#" + result.getName() + "\n");
    }

    @Override
    public void onTestFailure(ITestResult result) {
        System.out.println("Ending in Failure: " + result.getTestClass().getName() + "#" + result.getName() + "\n");
    }

    @Override
    public void onTestSkipped(ITestResult result) {

    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult result) {

    }

    @Override
    public void onStart(ITestContext context) {

    }

    @Override
    public void onFinish(ITestContext context) {

    }
}
