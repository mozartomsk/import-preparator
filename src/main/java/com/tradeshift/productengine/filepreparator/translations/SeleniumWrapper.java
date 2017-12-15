package com.tradeshift.productengine.filepreparator.translations;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

import java.io.Closeable;
import java.io.IOException;

public class SeleniumWrapper implements Closeable {

    private final WebDriver driver;

    static {
        System.setProperty("webdriver.chrome.driver", "/home/pkonstantinov/bin/chromedriver");
    }

    public SeleniumWrapper() {
        driver = new ChromeDriver();
    }

    public void openTranslatePage(String langFrom, String langTo) {
        String url = String.format("https://translate.google.ru/#%s/%s", langFrom, langTo);
        if (!getUrl().contains(url)) {
            driver.get(url);
        }
    }

    public void setSource(String source) {
        WebElement webElement = driver.findElement(By.id("source"));
        webElement.clear();
        webElement.sendKeys(source);
    }

    public String getResult() {
        return driver.findElement(By.id("result_box")).getText();
    }

    @Override
    public void close() throws IOException {
        driver.close();
        driver.quit();
    }

    /**
     * @return last read result
     */
    public String waitForResultChanged(long upToMillis) {
        String startingResult = getResult();
        String previousResult = startingResult;
        long waited = 0;

        do {
            try {
                Thread.sleep(100);
                waited += 100;
            } catch (InterruptedException ignore) {}

            String currentResult = getResult();

            if (currentResult.equals(previousResult) && !startingResult.equals(currentResult)
                    &&!currentResult.contains("...")) {
                return currentResult;
            }
            previousResult = currentResult;
        } while (waited < upToMillis);
        return getResult();
    }

    public String getUrl() {
        return driver.getCurrentUrl();
    }
}
