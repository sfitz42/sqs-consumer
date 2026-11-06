import pytest

from sqs_consumer import config, errors


class TestReceive:
    @pytest.mark.parametrize(
        ("option", "value"),
        [
            ("wait_time_seconds", 21),
            ("max_messages", 0),
            ("max_messages", 11),
            ("visibility_timeout", -1),
            ("visibility_timeout", 43201),
        ],
    )
    def test_rejects_out_of_range_settings(self, option, value):
        with pytest.raises(errors.ConfigurationError, match=option):
            config.Receive(**{option: value})


class TestHeartbeat:
    def test_rejects_an_interval_equal_to_the_visibility_timeout(self):
        with pytest.raises(errors.ConfigurationError, match="less than"):
            config.Heartbeat(interval=30, visibility_timeout=30)

    def test_rejects_a_visibility_timeout_beyond_twelve_hours(self):
        with pytest.raises(errors.ConfigurationError, match="43200"):
            config.Heartbeat(interval=10, visibility_timeout=43201)

    def test_rejects_a_non_positive_interval(self):
        with pytest.raises(errors.ConfigurationError, match="greater than 0"):
            config.Heartbeat(interval=0, visibility_timeout=30)


class TestReleaseOnError:
    def test_rejects_a_negative_visibility_timeout(self):
        with pytest.raises(errors.ConfigurationError, match="0..43200"):
            config.ReleaseOnError(visibility_timeout=-1)


class TestBackoff:
    def test_delay_grows_by_the_factor_until_the_maximum(self):
        backoff = config.Backoff(initial=1, maximum=5, factor=2, jitter=False)

        delays = [backoff.delay(failures) for failures in range(1, 6)]

        assert delays == [1, 2, 4, 5, 5]

    def test_jittered_delay_stays_between_zero_and_the_ceiling(self):
        backoff = config.Backoff(initial=1, maximum=5, factor=2, jitter=True)

        delays = [backoff.delay(3) for _ in range(200)]

        assert min(delays) >= 0
        assert max(delays) <= 4
        assert len(set(delays)) > 1

    def test_rejects_a_maximum_below_the_initial_delay(self):
        with pytest.raises(errors.ConfigurationError, match="maximum >= initial"):
            config.Backoff(initial=10, maximum=1)
