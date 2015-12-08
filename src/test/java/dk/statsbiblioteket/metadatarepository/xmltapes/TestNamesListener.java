package dk.statsbiblioteket.metadatarepository.xmltapes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestNGListener;
import org.testng.ITestResult;
import org.testng.internal.ConstructorOrMethod;

/**
 * Created by abr on 08-12-15.
 */
public class TestNamesListener implements ITestListener {

    private static final Logger log = LoggerFactory.getLogger(TestNamesListener.class);

    @Override
    public void onTestStart(ITestResult result) {
            ConstructorOrMethod constructorOrMethod = result.getMethod().getConstructorOrMethod();
            log.warn("Running " + constructorOrMethod.toString());

    }

    @Override
    public void onTestSuccess(ITestResult result) {
        ConstructorOrMethod constructorOrMethod = result.getMethod().getConstructorOrMethod();
        log.warn("Ending " + constructorOrMethod.toString());
    }

    @Override
    public void onTestFailure(ITestResult result) {
        ConstructorOrMethod constructorOrMethod = result.getMethod().getConstructorOrMethod();
        log.warn("Ending " + constructorOrMethod.toString());
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
