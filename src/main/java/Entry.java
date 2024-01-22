import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@AllArgsConstructor
@NoArgsConstructor
public class Entry {
    @Getter
    @Setter
    private String key;
    @Getter
    @Setter
    private String value;
    @Getter
    @Setter
    private Instant expiry_ms;

    @Override
    public String toString() {
        return String.format("key: %s, value: %s, expiry: %s", key, value, expiry_ms);
    }
}
