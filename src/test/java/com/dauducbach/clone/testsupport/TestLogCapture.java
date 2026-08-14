package com.dauducbach.clone.testsupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public final class TestLogCapture implements AutoCloseable {
    private final Logger logger;
    private final CapturingAppender appender;

    private TestLogCapture(Class<?> source) {
        logger = (Logger) LogManager.getLogger(source);
        appender = new CapturingAppender(source.getSimpleName() + "TestCapture");
        appender.start();
        logger.addAppender(appender);
    }

    public static TestLogCapture start(Class<?> source) {
        return new TestLogCapture(source);
    }

    public List<String> messages() {
        return List.copyOf(appender.messages);
    }

    @Override
    public void close() {
        logger.removeAppender(appender);
        appender.stop();
    }

    private static final class CapturingAppender extends AbstractAppender {
        private final List<String> messages = new CopyOnWriteArrayList<>();

        private CapturingAppender(String name) {
            super(name, null, PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }
    }
}
