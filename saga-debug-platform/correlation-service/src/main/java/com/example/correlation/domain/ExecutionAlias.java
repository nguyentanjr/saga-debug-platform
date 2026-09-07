package com.example.correlation.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * One identifier that names an execution already known by another.
 *
 * <h2>Scoped to a source</h2>
 * The key is the pair {@code (sourceName, aliasKey)}, not the identifier alone. An alias
 * table keyed on the bare string is a cross-system merge waiting to happen: two observed
 * systems that both use {@code PAY-9} would resolve to each other's executions, and the
 * platform would fold two unrelated runs into one and then diagnose the result. Scoping
 * makes that impossible rather than unlikely.
 *
 * <h2>Written only from evidence</h2>
 * A row appears when an observation carried both identifiers at once, and
 * {@code linkedByEventId} records which one. No timing window, no similarity between
 * values, no shared business key — a wrong link would need a producer to have emitted a
 * wrong pair.
 */
@Entity
@Table(name = "execution_aliases")
@IdClass(ExecutionAlias.Key.class)
public class ExecutionAlias {

    /** The namespace this alias belongs to; half of the primary key. */
    @Id
    @Column(name = "source_name")
    private String sourceName;

    /** The identifier being resolved, as its source published it — a payment id, a booking reference. */
    @Id
    @Column(name = "alias_key")
    private String aliasKey;

    /** The namespaced execution key every observation of this run is filed under. */
    @Column(name = "execution_id", nullable = false)
    private String executionId;

    /** The observation that carried both identifiers, which is the evidence for this row. */
    @Column(name = "linked_by_event_id")
    private String linkedByEventId;

    @Column(name = "observed_at", nullable = false)
    private Instant observedAt;

    protected ExecutionAlias() {
    }

    public ExecutionAlias(String sourceName, String aliasKey, String executionId,
            String linkedByEventId, Instant observedAt) {
        this.sourceName = sourceName;
        this.aliasKey = aliasKey;
        this.executionId = executionId;
        this.linkedByEventId = linkedByEventId;
        this.observedAt = observedAt;
    }

    public String getSourceName() { return sourceName; }

    public String getAliasKey() { return aliasKey; }

    public String getExecutionId() { return executionId; }
    public void setExecutionId(String executionId) { this.executionId = executionId; }

    public String getLinkedByEventId() { return linkedByEventId; }

    public Instant getObservedAt() { return observedAt; }

    /** The alias as an execution key in its own right. */
    public ExecutionKey key() {
        return ExecutionKey.of(sourceName, aliasKey);
    }

    /** The composite primary key: an alias means nothing outside the source that used it. */
    public static class Key implements Serializable {
        private String sourceName;
        private String aliasKey;

        public Key() {
        }

        public Key(String sourceName, String aliasKey) {
            this.sourceName = sourceName;
            this.aliasKey = aliasKey;
        }

        public static Key of(ExecutionKey key) {
            return new Key(key.source(), key.rawId());
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof Key that)) return false;
            return Objects.equals(sourceName, that.sourceName)
                    && Objects.equals(aliasKey, that.aliasKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceName, aliasKey);
        }
    }
}
