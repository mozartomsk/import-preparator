package com.tradeshift.productengine.filepreparator;


import au.com.bytecode.opencsv.CSVWriter;
import com.tradeshift.productengine.filepreparator.translations.Cache;
import com.tradeshift.productengine.filepreparator.translations.Pool;
import com.tradeshift.productengine.filepreparator.translations.SeleniumWrapper;
import lombok.Data;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.jsoup.Jsoup;

import java.io.*;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleExcelReaderExample {

    static final int THREAD_COUNT = 10;
    static final int SELENIUM_COUNT = 4;

    private static final List<String> languages = Arrays.asList("en", "sv", "de", /*"ru",*/ "fr");

    private final static BlockingQueue blockingQueue = new LinkedBlockingQueue(THREAD_COUNT + 5);
    private final static ThreadPoolExecutor executor = new ThreadPoolExecutor(THREAD_COUNT, THREAD_COUNT,
            60, TimeUnit.SECONDS, blockingQueue);
    private final static LinkedBlockingQueue<Future<?>> futureQueue = new LinkedBlockingQueue<>();

    private final static DecimalFormat moneyFormat = new DecimalFormat("#0.00");
    private final static DecimalFormat intFormat = new DecimalFormat("#0");


    public static void main(String[] args) throws IOException, InterruptedException {
        TreeMap<String, RowBean> rowBeans = readBeans("/home/pkonstantinov/Documents/United product list.xlsx");

        try (Cache cache = new Cache("/home/pkonstantinov/tradeshift/my-forked/importpreparator/db/translations.db","translations");
             Pool<SeleniumWrapper> seleniumWrapperPool = new Pool<>(SELENIUM_COUNT, SeleniumWrapper::new);
        ) {
            fillTranslations(cache, seleniumWrapperPool, rowBeans);
        }
        try (Writer writer = new FileWriter("/home/pkonstantinov/Documents/feed.csv");
                CSVWriter csvWriter = new CSVWriter(writer)
        ) {
            exportToCsv(rowBeans, csvWriter);
        }
    }

    private static void exportToCsv(TreeMap<String, RowBean> rowBeans, CSVWriter csvWriter) throws IOException {
        csvWriter.writeNext(new String[] {"SKU*", "Language", "Name*", "Price*", "Currency", "Category", "Description", "Image1",
                "Image2", "Image3", "Image4", "Image5", "UOM"});

        for (RowBean rowBean : rowBeans.values()) {
            for (Map.Entry<String, LanguagePart> languageEntry : rowBean.getLanguageParts().entrySet()) {
                csvWriter.writeNext(new String[] {
                        getEmptyNull(rowBean.getIdentifier()),
                        getEmptyNull(languageEntry.getKey()),
                        getEmptyNull(languageEntry.getValue().getTitle()),
                        getEmptyNull(moneyFormat.format(rowBean.getPrice())),
                        getEmptyNull(rowBean.getCurrency()),
                        getEmptyNull(rowBean.getCategory()),
                        getEmptyNull(languageEntry.getValue().getDescription()),
                        getEmptyNull(rowBean.getImage1()),
                        getEmptyNull(rowBean.getImage2()),
                        getEmptyNull(rowBean.getImage3()),
                        getEmptyNull(rowBean.getImage4()),
                        getEmptyNull(rowBean.getImage5()),
                        getEmptyNull(rowBean.getUom())
                });
            }
        }
    }

    private static String getEmptyNull(String source) {
        return source == null ? "" : source;
    }

    private static void fillTranslations(Cache cache, Pool<SeleniumWrapper> seleniumWrapperPool, TreeMap<String, RowBean> rowBeans) {
        long totalCapacity = calcTotalCapacity(rowBeans);
        final AtomicLong counter = new AtomicLong();
        System.out.println("Number of needed translations: " + totalCapacity);

        for (String fromLanguage : languages) {
            for (String toLanguage : languages) {

                for (RowBean rowBean : rowBeans.values()) {
                    final LanguagePart from = rowBean.getLanguagePart(fromLanguage);
                    final LanguagePart to = rowBean.getLanguagePart(toLanguage);
                    if (isNotBlank(from.getTitle()) && isBlank(to.getTitle()) && from.isOrigin()) {
                        futureQueue.add(executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                synchronized (to) {
                                    to.setTitle(translate(cache, seleniumWrapperPool, fromLanguage, toLanguage, from.getTitle()));
                                    if (counter.incrementAndGet() % (totalCapacity / 100) == 0) {
                                        System.out.print(".");
                                    }
                                }
                            }
                        }));
                    }
                    if (isNotBlank(from.getDescription()) && isBlank(to.getDescription()) && from.isOrigin()) {
                        futureQueue.add(executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                to.setDescription(translate(cache, seleniumWrapperPool, fromLanguage, toLanguage, from.getDescription()));
                                if (counter.incrementAndGet() % (totalCapacity / 100) == 0) {
                                    System.out.print(".");
                                }
                            }
                        }));
                    }
                    while (futureQueue.size() >= THREAD_COUNT) {
                        Future future = futureQueue.poll();
                        if (future != null) {
                            try {
                                future.get();
                            } catch (InterruptedException | ExecutionException e) {
                                throw new IllegalStateException(e);
                            }
                        }
                    }
                }
            }
        }

        for (Future<?> future : futureQueue) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private static long calcTotalCapacity(TreeMap<String, RowBean> rowBeans) {
        long result = 0;
        for (String toLanguage : languages) {
            for (RowBean rowBean : rowBeans.values()) {
                if (isBlank(rowBean.getLanguagePart(toLanguage).getTitle())) {
                    result++;
                }
                if (isBlank(rowBean.getLanguagePart(toLanguage).getDescription())) {
                    result++;
                }
            }
        }
        return result;
    }

    private static String translate(Cache cache, Pool<SeleniumWrapper> seleniumWrapperPool, String fromLanguage, String toLanguage, String source) {

        if (cache.containsKey(fromLanguage, toLanguage, source)) {
            String res = cache.get(fromLanguage, toLanguage, source);
            if (isNotBlank(res)) {
                return res;
            }
        }

        SeleniumWrapper seleniumWrapper = seleniumWrapperPool.take();
        try {
            seleniumWrapper.openTranslatePage(fromLanguage, toLanguage);
            seleniumWrapper.setSource(source);
            String result = seleniumWrapper.waitForResultChanged(2 * 1000);
            cache.put(fromLanguage, toLanguage, source, result);
            return result;
        } finally {
            seleniumWrapperPool.setFree(seleniumWrapper);
        }
    }

    private static TreeMap<String, RowBean> readBeans(String excelFilePath) throws IOException {
        try (
            FileInputStream inputStream = new FileInputStream(new File(excelFilePath));
            Workbook workbook = new XSSFWorkbook(inputStream)
        ) {
            Sheet firstSheet = workbook.getSheetAt(0);
            Iterator<Row> iterator = firstSheet.iterator();

            if (iterator.hasNext()) {
                iterator.next();
            }

            TreeMap<String, RowBean> rowBeans = new TreeMap<>(Comparator.comparing(String::toString));

            while (iterator.hasNext()) {
                Row nextRow = iterator.next();
                RowBean rowBean = new RowBean();
                readRowBean(nextRow, rowBean);

                if (rowBeans.containsKey(rowBean.getIdentifier())) {
                    rowBean = rowBeans.get(rowBean.getIdentifier());
                    readRowBean(nextRow, rowBean);
                } else {
                    rowBeans.put(rowBean.getIdentifier(), rowBean);
                }
            }

            return rowBeans;
        }
    }

    private static RowBean readRowBean(Row nextRow, RowBean rowBean) {
        Iterator<Cell> cellIterator = nextRow.cellIterator();

        String language = null;

        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();

            switch (cell.getCellType()) {
                case Cell.CELL_TYPE_STRING:
                    switch (cell.getColumnIndex()) {
                        case 0:
                            rowBean.setIdentifier(cell.getStringCellValue());
                            break;
                        case 1:
                            language = cell.getStringCellValue();
                            break;
                        case 2:
                            LanguagePart languagePart = rowBean.getLanguagePart(language);
                            languagePart.setTitle(cell.getStringCellValue());
                            languagePart.setOrigin(true);
                            break;
                        case 3:
                            rowBean.setPrice(new BigDecimal(cell.getStringCellValue()));
                            break;
                        case 4:
                            rowBean.setCurrency(cell.getStringCellValue());
                            break;
                        case 5:
                            rowBean.setCategory(cell.getStringCellValue());
                            break;
                        case 6:
                            rowBean.getLanguagePart(language).setDescription(Jsoup.parse(cell.getStringCellValue()).text());
                            break;
                        case 7:
                            rowBean.setImage1(cell.getStringCellValue());
                            break;
                        case 8:
                            rowBean.setImage2(cell.getStringCellValue());
                            break;
                        case 9:
                            rowBean.setImage3(cell.getStringCellValue());
                            break;
                        case 10:
                            rowBean.setImage4(cell.getStringCellValue());
                            break;
                        case 11:
                            rowBean.setImage5(cell.getStringCellValue());
                            break;
                        case 12:
                            rowBean.setUom(cell.getStringCellValue());
                            break;
                        default:
                            throw new IllegalStateException("Unknown columnNumber: " + cell.getColumnIndex());
                    }
                    break;
                case Cell.CELL_TYPE_NUMERIC:
                    switch (cell.getColumnIndex()) {
                        case 0:
                            rowBean.setIdentifier(intFormat.format(cell.getNumericCellValue()));
                            break;
                        case 3:
                            rowBean.setPrice(new BigDecimal(cell.getNumericCellValue()));
                            break;
                        case 5:
                            rowBean.setCategory(intFormat.format(cell.getNumericCellValue()));
                            break;
                        default:
                            throw new IllegalStateException("Unexpected numeric value in columnNumber: " + cell.getColumnIndex());
                    }
                    break;
                case Cell.CELL_TYPE_BLANK:
                    break;
                default: throw new IllegalStateException("Unexpected value type in columnNumber: " + cell.getColumnIndex() +
                        ", type: " + cell.getCellType());
            }
        }
        return rowBean;
    }

    @Data
    private static class RowBean {
        /*
        SKU*	Language	Name*	Price*	Currency	Category	Description	Image1	Image2	Image3	Image4	Image5	UOM
         */
        private String identifier, currency, category, image1, image2, image3, image4, image5, uom;
        private BigDecimal price;
        private final Map<String, LanguagePart> languageParts = new HashMap<>();

        LanguagePart getLanguagePart(String language) {
            notBlank(language);
            if (languageParts.containsKey(language)) {
                return languageParts.get(language);
            } else {
                LanguagePart languagePart = new LanguagePart();
                languageParts.put(language, languagePart);
                return languagePart;
            }
        }
    }

    @Data
    private static class LanguagePart {
        private String title, description;
        private boolean origin;
    }

    private static void notBlank(String source) {
        if (isBlank(source)) {
            throw new IllegalArgumentException();
        }
    }

    private static boolean isNotBlank(String source) {
        return !isBlank(source);
    }

    private static boolean isBlank(String source) {
        return source == null || source.trim().length() == 0;
    }
}
