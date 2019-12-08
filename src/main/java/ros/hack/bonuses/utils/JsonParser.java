package ros.hack.bonuses.utils;

import com.github.voteva.Operation;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;

@Slf4j
@Component
@RequiredArgsConstructor
public class JsonParser {
    private static final Gson gson = new GsonBuilder().create();

    @Nonnull
    public static Operation parse(@Nonnull String message) {
        if (log.isDebugEnabled()) {
            log.debug(message);
        }
        return gson.fromJson(message, Operation.class);
    }
}
