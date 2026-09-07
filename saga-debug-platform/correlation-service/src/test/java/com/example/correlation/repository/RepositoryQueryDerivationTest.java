package com.example.correlation.repository;

import com.example.correlation.domain.Anomaly;
import com.example.correlation.domain.RcaResult;
import com.example.correlation.domain.SagaEvent;
import com.example.correlation.domain.SagaInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.repository.query.parser.PartTree;

import java.lang.reflect.Method;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Checks that every derived query name resolves against its entity.
 *
 * <p>Spring Data parses these names when the application context starts, so a typo or a
 * property that does not exist is a startup failure rather than a compile error. Nothing
 * else in this suite boots a context — that needs Postgres and Kafka — so the parse is
 * exercised directly here instead.
 */
class RepositoryQueryDerivationTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "findByStatus",
            "findByStatusAndLastUpdatedBefore",
            "countByStartTimeAfter",
            "countByStatusAndStartTimeAfter",
            "findByStartTimeBetween",
            "findByStatusAndStartTimeBetween",
            "findByStatusIn",
            "findByStatusInAndLastUpdatedAfter",
            "findByStatusInAndEndTimeAfter",
            "findByStatusInAndLastUpdatedBefore",
    })
    void sagaInstanceQueriesResolve(String methodName) {
        assertThatCode(() -> new PartTree(methodName, SagaInstance.class))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "findBySagaIdOrderByTimestampAsc",
            "findBySagaIdOrderByReceivedAtAsc",
            "findBySourceNameAndEventId",
            "countBySagaId",
            "deleteByTimestampBefore",
            "findTopBySagaIdOrderByTimestampDesc",
            "findBySagaIdInOrderBySagaIdAscTimestampAsc",
            "deleteBySagaIdIn",
    })
    void sagaEventQueriesResolve(String methodName) {
        assertThatCode(() -> new PartTree(methodName, SagaEvent.class))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "findBySagaId",
            "findByDetectedAtAfter",
            "existsBySagaIdAndAnomalyType",
            "deleteByDetectedAtBefore",
            "findBySagaIdIn",
            "deleteBySagaIdIn",
    })
    void anomalyQueriesResolve(String methodName) {
        assertThatCode(() -> new PartTree(methodName, Anomaly.class))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "findBySagaIdOrderByCreatedAtDesc",
            "findByCreatedAtAfter",
            "deleteAllBySagaId",
            "deleteBySagaIdIn",
    })
    void rcaResultQueriesResolve(String methodName) {
        assertThatCode(() -> new PartTree(methodName, RcaResult.class))
                .doesNotThrowAnyException();
    }

    /**
     * Guards the list above against drifting out of date: every derived method declared
     * on the repositories must appear in one of the parameterised sets.
     */
    @Test
    void everyDeclaredDerivedQueryIsCovered() {
        List<Class<?>> repositories = List.of(
                SagaInstanceRepository.class, SagaEventRepository.class,
                AnomalyRepository.class, RcaResultRepository.class);

        List<String> covered = List.of(
                "findByStatus", "findByStatusAndLastUpdatedBefore", "countByStartTimeAfter",
                "countByStatusAndStartTimeAfter", "findByStartTimeBetween",
                "findByStatusAndStartTimeBetween", "findByStatusIn",
                "findByStatusInAndLastUpdatedAfter", "findByStatusInAndEndTimeAfter",
                "findByStatusInAndLastUpdatedBefore", "findBySagaIdOrderByTimestampAsc",
                "findBySagaIdOrderByReceivedAtAsc", "findBySourceNameAndEventId", "countBySagaId",
                "deleteByTimestampBefore", "findTopBySagaIdOrderByTimestampDesc",
                "findBySagaIdInOrderBySagaIdAscTimestampAsc", "deleteBySagaIdIn",
                "findBySagaId", "findByDetectedAtAfter", "existsBySagaIdAndAnomalyType",
                "deleteByDetectedAtBefore", "findBySagaIdIn",
                "findBySagaIdOrderByCreatedAtDesc", "findByCreatedAtAfter", "deleteAllBySagaId");

        List<String> declared = repositories.stream()
                .flatMap(repo -> java.util.Arrays.stream(repo.getDeclaredMethods()))
                // @Query methods carry their own JPQL and are not name-derived.
                .filter(m -> m.getAnnotation(org.springframework.data.jpa.repository.Query.class) == null)
                .map(Method::getName)
                .distinct()
                .toList();

        assertThat(declared).isSubsetOf(covered);
    }
}
