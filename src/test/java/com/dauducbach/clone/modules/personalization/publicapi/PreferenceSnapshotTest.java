package com.dauducbach.clone.modules.personalization.publicapi;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PreferenceSnapshotTest {
    @Test
    void snapshotCopiesEachVectorAndExposesNoMutableState() {
        List<Double> profile = new ArrayList<>(List.of(1d));
        List<Double> longTerm = new ArrayList<>(List.of(2d));
        List<Double> shortTerm = new ArrayList<>(List.of(3d));

        PreferenceSnapshot snapshot = new PreferenceSnapshot(7, profile, longTerm, shortTerm, true, "model-v1");
        profile.set(0, 9d);
        longTerm.set(0, 9d);
        shortTerm.set(0, 9d);

        assertThat(snapshot.profile()).containsExactly(1d);
        assertThat(snapshot.longTerm()).containsExactly(2d);
        assertThat(snapshot.shortTerm()).containsExactly(3d);
        assertThatThrownBy(() -> snapshot.profile().set(0, 4d))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
