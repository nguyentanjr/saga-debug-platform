package com.example.correlation.domain;

import java.util.Objects;

/**
 * Which execution, of which observed system.
 *
 * <h2>Why an identifier alone is not one</h2>
 * Executions used to be keyed by the raw identifier the source published, on the unstated
 * assumption that identifiers are unique across everything the platform watches. They are
 * not. Two systems observed by one deployment are two separate companies' id spaces:
 * {@code ORD-1} in an order system and {@code ORD-1} in a partner's system are different
 * runs that happen to share a string.
 *
 * <p>Filing them together did not fail loudly. It produced one fabricated execution whose
 * events interleaved two systems, and the detectors then reported perfectly true things
 * about a run that never happened — a terminal saga resuming, a state regression, a step
 * timing out after twelve hours. Every one of those findings was manufactured by the
 * observer and blamed on the observed.
 *
 * <p>So identity is the pair. The source is not metadata attached to an execution; it is
 * half of what names it.
 *
 * <h2>Why the pair is carried as one string</h2>
 * The canonical form {@code source::rawId} is what reaches the database, the API and the
 * dashboard, so a single column stays the primary key and every existing query, path
 * variable and link keeps working. Uniqueness is then a property of the value rather than
 * of a constraint someone has to remember to write, and {@code source_name} is stored
 * beside it with a CHECK tying the two together, so the invariant survives writers that
 * never go through this class.
 *
 * @param source the mapping that read the observation — the namespace
 * @param rawId  the identifier as the observed system published it, unchanged
 */
public record ExecutionKey(String source, String rawId) {

    /**
     * Separates the two halves.
     *
     * <p>Two colons rather than one because single colons are ordinary inside real
     * identifiers — URNs, Kafka keys, prefixed business references. Source names are
     * checked for this sequence when a mapping is loaded, so the split below is
     * unambiguous by construction rather than by hope.
     */
    public static final String SEPARATOR = "::";

    /** The name given to observations from a source that declares no mapping. */
    public static final String DEFAULT_SOURCE = "default";

    public ExecutionKey {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(rawId, "rawId");
        if (source.isBlank()) {
            throw new IllegalArgumentException("source must not be blank");
        }
        if (rawId.isBlank()) {
            throw new IllegalArgumentException("rawId must not be blank");
        }
        if (source.contains(SEPARATOR)) {
            throw new IllegalArgumentException(
                    "source name must not contain '" + SEPARATOR + "': " + source);
        }
    }

    public static ExecutionKey of(String source, String rawId) {
        return new ExecutionKey(source, rawId);
    }

    /**
     * Reads a key back from its stored form.
     *
     * <p>Splits at the first separator, so a raw id containing one is preserved whole.
     *
     * @throws IllegalArgumentException when the value carries no namespace, which means
     *                                  it was written by something that bypassed this
     *                                  class — worth failing on rather than guessing at
     */
    public static ExecutionKey parse(String canonical) {
        if (canonical == null || canonical.isBlank()) {
            throw new IllegalArgumentException("no execution key to parse");
        }
        int at = canonical.indexOf(SEPARATOR);
        if (at < 0) {
            throw new IllegalArgumentException(
                    "not a namespaced execution key: '" + canonical + "'");
        }
        return new ExecutionKey(canonical.substring(0, at),
                canonical.substring(at + SEPARATOR.length()));
    }

    /** The source half of a stored key, or null when the value carries no namespace. */
    public static String sourceOf(String canonical) {
        if (canonical == null) return null;
        int at = canonical.indexOf(SEPARATOR);
        return at < 0 ? null : canonical.substring(0, at);
    }

    public boolean sameSourceAs(ExecutionKey other) {
        return other != null && source.equals(other.source);
    }

    /** The stored form: what goes in {@code saga_id} and travels through the API. */
    public String canonical() {
        return source + SEPARATOR + rawId;
    }

    @Override
    public String toString() {
        return canonical();
    }
}
