# Repository Guidelines

## Project Structure & Module Organization

This is a Java 21 Spring Boot WebFlux backend. Main source code lives under `src/main/java/com/dauducbach/clone`. The application entry point is `CloneApplication.java`.

Core packages are organized by responsibility: shared API responses, constants, and exceptions in `commons`; Spring configuration in `configuration`; cross-cutting helpers in `utils`; infrastructure services in `infrastructure`; and feature code in `modules`. Feature modules include `auth`, `user`, `post`, `feed`, `notification`, `audit`, and `chat`, with common subfolders such as `controller`, `service`, `entity`, `dto`, `repository` or the existing `repositoty` spelling. Tests mirror production packages under `src/test/java/com/dauducbach/clone`. Runtime configuration is in `src/main/resources/application.yaml`, logging in `log4j2.xml`, and manual SQL scripts in `src/main/resources/db/manual`.
The `modules/frontend` package is only an API composition layer for screens that need aggregated data assembled from multiple domain modules. Do not place entities, repositories, or domain-owned CRUD services in `modules/frontend`. Code that directly belongs to a feature must live in that feature module: saved collections, saved items, drafts, and archive items belong to `modules/post`; notification listing, read state, push token, template, and notification delivery code belongs to `modules/notification`; user settings and user profile ownership belong to `modules/user`. Frontend controllers may delegate to domain services when keeping an existing FE route stable, but persistence and business rules stay in the owning module.

## Build, Test, and Development Commands

- `.\mvnw.cmd clean test`: run the full test suite from a clean build.
- `.\mvnw.cmd test`: run unit and slice tests.
- `.\mvnw.cmd spring-boot:run`: start the API locally using `application.yaml`.
- `.\mvnw.cmd clean package`: build the application artifact under `target`.

Use the Maven wrapper on Windows so contributors use the project-pinned Maven setup.

## Coding Style & Naming Conventions

Use 4-space indentation for Java. Keep class names in `PascalCase`, methods and fields in `camelCase`, constants in `UPPER_SNAKE_CASE`, and packages lowercase. Follow the existing module pattern: controllers expose HTTP endpoints, services hold business logic, repositories isolate persistence, DTOs are split into `request`, `response`, and `event` where relevant. Prefer Reactor types consistently in WebFlux flows and avoid blocking calls in reactive paths.

## Testing Guidelines

Tests use Spring Boot Test, JUnit 5, Reactor Test, H2, and Spring Security Test. Name test classes with the `*Test` suffix, matching the class or behavior under test, for example `PostServiceTest` or `UserSearchControllerTest`. Add focused tests beside changed module code, especially for services, controllers, security behavior, and reactive flows. Run `.\mvnw.cmd test` before opening a pull request.

## Commit & Pull Request Guidelines

Recent commits use short, imperative summaries such as `add user details module` or `search suggestion and refactor media scan`. Keep commits focused on one feature or fix. Pull requests should include a brief description, linked issue when available, test evidence, configuration or migration notes, and API examples or Postman updates when endpoints change.

## Security & Configuration Tips

Do not commit secrets, tokens, private keys, or local credentials. Keep environment-specific values out of `application.yaml` unless they are safe defaults. Review changes touching security, OAuth, cookies, Firebase, Cloudinary, Kafka, Redis, Elasticsearch, or database connection settings carefully.
## Messaging Surface Parity

The full messaging page and floating mini-chat must use the same shared message flow. Any backend or frontend change to message payloads, sending, media/audio, replies, realtime delivery, unread state, delivery/read cursors, status transitions, offline behavior, or error recovery must be applied and verified for both surfaces. Surface-specific code may only handle presentation concerns; shared chat behavior must not be duplicated in separate page-chat and mini-chat implementations.
