package ru.lunidep.kafkaproducer.service;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import org.springframework.beans.factory.annotation.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.lunidep.kafkaproducer.models.CustomerData;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class CsvReaderService {

    @Value("${app.data-directory}")
    private String dataDirectory;

    public List<CustomerData> readAllCsvFiles() throws IOException {
        List<CustomerData> allData = new ArrayList<>();
        Path dataPath = Paths.get(dataDirectory);
        log.debug("Поиск CSV файлов в директории: {}", dataDirectory);

        List<Path> csvFiles = Files.list(dataPath)
                .filter(path -> {
                    boolean isCsv = path.toString().toLowerCase().endsWith(".csv");
                    if (isCsv) {
                        log.debug("Найден CSV файл: {}", path.getFileName());
                    }
                    return isCsv;
                })
                .toList();

        for (Path csvFile : csvFiles) {
            try {
                log.info("Обработка файла: {}", csvFile.getFileName());
                List<CustomerData> fileData = readCsvFile(csvFile);
                allData.addAll(fileData);
                log.info("Успешно прочитано {} записей из файла: {}", fileData.size(), csvFile.getFileName());
            } catch (IOException e) {
                log.error("Ошибка при чтении файла {}: {}", csvFile.getFileName(), e.getMessage());
            }
        }

        return allData;
    }

    private List<CustomerData> readCsvFile(Path filePath) throws IOException {

        try (FileReader reader = new FileReader(filePath.toFile())) {
            CsvToBean<CustomerData> csvToBean = new CsvToBeanBuilder<CustomerData>(reader)
                    .withType(CustomerData.class)
                    .withIgnoreLeadingWhiteSpace(true)
                    .withIgnoreEmptyLine(true)
                    .build();

            List<CustomerData> result = csvToBean.parse();
            log.debug("Из файла {} извлечено {} записей", filePath.getFileName(), result.size());
            return result;
        }
    }
}
