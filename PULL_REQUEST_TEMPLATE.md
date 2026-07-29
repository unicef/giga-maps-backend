# Make metrics endpoint resilient and guard get_git_hash; add tests

Summary:
- Adds robust error handling and a timeout to the Flask /metrics endpoint (returns 503 when upstream is unavailable and preserves Content-Type).
- Makes config.utils.get_git_hash safe when gitpython is missing or not in a git repo (returns 'unknown' instead of raising).
- Adds pytest tests for both code paths.

Why:
Prevents health-check hangs and import-time crashes in environments without git metadata or gitpython.
