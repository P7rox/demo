package com.example.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

@SpringBootApplication
public class DemoApplication implements CommandLineRunner {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    @Qualifier("hiveJdbcTemplate")
    private JdbcTemplate hiveJdbcTemplate;

    public String select() {
        String sql = "select * from HIVE_TEST";
        List<Map<String, Object>> rows = hiveJdbcTemplate.queryForList(sql);
        System.out.println(hiveJdbcTemplate.queryForList(sql));
        System.out.println(rows);
        Iterator<Map<String, Object>> it = rows.iterator();
        while (it.hasNext()) {
            Map<String, Object> row = it.next();
            System.out.println(String.format("%s\t%s", row.get("hive_test.key"), row.get("hive_test.value")));
        }
        return "Done";
    }

    public void create() {

        StringBuffer sql = new StringBuffer("create table IF NOT EXISTS ");
        sql.append("HIVE_TEST");
        sql.append("(KEY INT, VALUE STRING)"); // partitioned storage
        sql.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' "); // 定义分隔符
        sql.append ("STORED AS TEXTFILE"); // as text storage

        logger.info(sql.toString());
        hiveJdbcTemplate.execute(sql.toString());

    }

    public void createDb() {

        StringBuffer sql = new StringBuffer("create database IF NOT EXISTS person_example");

        logger.info(sql.toString());
        hiveJdbcTemplate.execute(sql.toString());

    }

    public String insert() {
        hiveJdbcTemplate.execute("insert into hive_test(key, value) values(2,'sweet_heart')"
                );
        return "Done";
    }

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }


    @Override
    public void run(String... args) throws Exception {
        createDb();
        create();
        logger.info("insert users -> {}", insert());
        logger.info("All users -> {}", select());
    };
}
