package com.example.correlation.repository;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventTimeSourceMigrationTest {
    @Test
    void migrationKeepsOldRowsUnknownAndAllowsOldWritersWithoutInventingProvenance() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "");
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("CREATE TABLE saga_events (id INT PRIMARY KEY, timestamp TIMESTAMP WITH TIME ZONE)");
        jdbc.update("INSERT INTO saga_events VALUES (1, CURRENT_TIMESTAMP)");

        new ResourceDatabasePopulator(new ClassPathResource("db/migration/V17__event_time_source.sql"))
                .execute(dataSource);

        assertThat(jdbc.queryForObject("SELECT event_time_source FROM saga_events WHERE id = 1", String.class))
                .isNull();
        jdbc.update("INSERT INTO saga_events (id, timestamp) VALUES (2, CURRENT_TIMESTAMP)");
        assertThat(jdbc.queryForObject("SELECT event_time_source FROM saga_events WHERE id = 2", String.class))
                .isNull();
        jdbc.update("UPDATE saga_events SET event_time_source = 'DECLARED' WHERE id = 1");
        jdbc.update("UPDATE saga_events SET event_time_source = 'TRANSPORT' WHERE id = 2");
        assertThatThrownBy(() -> jdbc.update("UPDATE saga_events SET event_time_source = 'GUESSED' WHERE id = 1"))
                .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
    }
}
