# gmq documentation

These guides adapt the [original wiki](https://github.com/giant-stone/gmq/wiki) to the current API.
The wiki's gomock page was a placeholder; the testing guide adds a runnable example.

- [Features and comparison](features.md)
- [Getting started](getting-started.md)
- [Message lifecycle and deduplication](message-lifecycle.md)
- [Custom consume rate](consume-rate.md)
- [Built-in and custom brokers](brokers.md)
- [Scheduling and operations](operations.md)
- [Testing with gomock](testing.md)

The [crawler example](../examples/crawl) demonstrates pacing and stable IDs.
Its HTML parser depends on the external site's markup.
