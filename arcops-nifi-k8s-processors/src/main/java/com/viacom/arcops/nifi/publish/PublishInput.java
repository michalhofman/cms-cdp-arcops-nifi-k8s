package com.viacom.arcops.nifi.publish;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Wither;

@Builder(toBuilder = true)
@Getter
@ToString
@Wither
@AllArgsConstructor
public class PublishInput {

    private final ImmutableList<String> uuids;
    private final String site;
    private final String stage;
    private final ArcConfig arcConfig;
    private final String username;
    private final Boolean unpublish;

    public PublishInput merge(PublishInput other) {
        if (other == null) {
            return this;
        }
        PublishInputBuilder builder = toBuilder();
        if (site == null) {
            builder.site(other.site);
        }
        if (stage == null) {
            builder.stage(other.stage);
        }
        if (arcConfig == null) {
            builder.arcConfig(other.arcConfig);
        } else if (other.arcConfig != null) {
            builder.arcConfig(arcConfig.merge(other.arcConfig));
        }
        if (username == null) {
            builder.username(other.username);
        }
        if (unpublish == null) {
            builder.unpublish(other.unpublish);
        }
        return builder.build();
    }
}
