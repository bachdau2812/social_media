package com.dauducbach.clone.modules.personalization.publicapi;

/** The caller no longer owns the consistency fence for this user's preference state. */
public class PreferenceLeaseLostException extends RuntimeException {
    public PreferenceLeaseLostException() {
        super("Preference consistency lease lost; reconcile pending state before retrying");
    }
}
