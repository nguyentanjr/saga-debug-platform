package com.example.correlation.ingestion;

import com.example.correlation.domain.ExecutionKey;

import java.time.Instant;

/**
 * An alias resolver for tests about something else.
 *
 * <p>Most sources declare no {@code aliasKey}, so for them the resolver is a pass-through
 * and the tests covering mapping, transports or detection have no interest in it. This
 * saves each of them a mock and a stubbing line, and keeps them from touching a database.
 *
 * <p>{@link ExecutionAliasResolverTest} is where the resolving is actually tested, and
 * {@link SourceScopedIdentityTest} is where the namespacing is.
 */
public class PassthroughAliasResolver extends ExecutionAliasResolver {

    public PassthroughAliasResolver() {
        super(null, null, null, null, null);
    }

    @Override
    public ExecutionKey canonical(ExecutionKey declared, ExecutionKey alias,
            String eventId, Instant observedAt) {
        return declared;
    }
}
