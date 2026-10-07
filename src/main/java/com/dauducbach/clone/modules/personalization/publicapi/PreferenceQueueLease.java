package com.dauducbach.clone.modules.personalization.publicapi;

/**
 * Short-lived fence for atomic feed queue scripts. The key names and token must only be used by
 * the callback passed to {@link PreferenceQueueConsistency#withSnapshotLease}; never persist or log them.
 */
public record PreferenceQueueLease(
        String userId,
        String ownershipToken,
        String lockKey,
        String versionKey,
        String dirtyKey
) {
}
