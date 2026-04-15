import schedlock


def test_version_is_defined():
    assert hasattr(schedlock, "__version__")
    assert isinstance(schedlock.__version__, str)


def test_public_exports():
    assert hasattr(schedlock, "schedlock")
    assert hasattr(schedlock, "FileBackend")
    assert hasattr(schedlock, "RedisBackend")


def test_schedlock_is_callable():
    assert callable(schedlock.schedlock)


def test_file_backend_importable():
    from schedlock import FileBackend
    assert FileBackend is not None


def test_redis_backend_importable():
    from schedlock import RedisBackend
    assert RedisBackend is not None
