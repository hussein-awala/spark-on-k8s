from __future__ import annotations

import datetime
from typing import Callable

import pytest
from freezegun import freeze_time
from spark_on_k8s.client import SparkOnK8S, default_app_id_suffix

FAKE_TIME = datetime.datetime(2024, 1, 14, 12, 12, 31)


def empty_suffix() -> str:
    return ""


class TestSparkOnK8s:
    @pytest.mark.parametrize(
        "app_name, app_id_suffix, expected_app_name, expected_app_id",
        [
            pytest.param(
                "spark-app-name",
                empty_suffix,
                "spark-app-name",
                "spark-app-name",
                id="app_name_without_suffix",
            ),
            pytest.param(
                "spark-app-name",
                default_app_id_suffix,
                "spark-app-name",
                "spark-app-name-20240114121231",
                id="app_name_with_suffix",
            ),
            pytest.param(
                "some-very-long-name-which-is-not-allowed-by-k8s-which-is-why-we-need-to-truncate-it",
                empty_suffix,
                "some-very-long-name-which-is-not-allowed-by-k8s-which-is-why-we",
                "some-very-long-name-which-is-not-allowed-by-k8s-which-is-why-we",
                id="app_name_without_suffix_long",
            ),
            pytest.param(
                "some-very-long-name-which-is-not-allowed-by-k8s-which-is-why-we-need-to-truncate-it",
                default_app_id_suffix,
                "some-very-long-name-which-is-not-allowed-by-k8s-w",
                "some-very-long-name-which-is-not-allowed-by-k8s-w-20240114121231",
                id="app_name_with_suffix_long",
            ),
            pytest.param(
                "some-name-ends-with-invalid-character-",
                empty_suffix,
                "some-name-ends-with-invalid-character",
                "some-name-ends-with-invalid-character",
                id="app_name_without_suffix_invalid_character",
            ),
            pytest.param(
                "some-name-ends-with-invalid-character-",
                default_app_id_suffix,
                "some-name-ends-with-invalid-character",
                "some-name-ends-with-invalid-character-20240114121231",
                id="app_name_with_suffix_invalid_character",
            ),
            pytest.param(
                "some.invalid_characters-in/the-name",
                empty_suffix,
                "some-invalid-characters-in-the-name",
                "some-invalid-characters-in-the-name",
                id="app_name_without_suffix_invalid_characters",
            ),
            pytest.param(
                "some.invalid_characters-in/the-name",
                default_app_id_suffix,
                "some-invalid-characters-in-the-name",
                "some-invalid-characters-in-the-name-20240114121231",
                id="app_name_with_suffix_invalid_characters",
            ),
            pytest.param(
                "name.with_8.numerical-characters/12345678",
                empty_suffix,
                "name-with-8-numerical-characters-12345678",
                "name-with-8-numerical-characters-12345678",
                id="app_name_without_suffix_numerical_characters",
            ),
            pytest.param(
                "name.with_8.numerical-characters/12345678",
                default_app_id_suffix,
                "name-with-8-numerical-characters-12345678",
                "name-with-8-numerical-characters-12345678-20240114121231",
                id="app_name_with_suffix_numerical_characters",
            ),
            pytest.param(
                "./-_---name-with-trailing-and-leading-dashes-_-_-,;",
                empty_suffix,
                "name-with-trailing-and-leading-dashes",
                "name-with-trailing-and-leading-dashes",
                id="app_name_without_suffix_trailing_and_leading_dashes",
            ),
            pytest.param(
                "./-_---name-with-trailing-and-leading-dashes-_-_-,;",
                default_app_id_suffix,
                "name-with-trailing-and-leading-dashes",
                "name-with-trailing-and-leading-dashes-20240114121231",
                id="app_name_with_suffix_trailing_and_leading_dashes",
            ),
            pytest.param(
                "12345-name-starts-with-numbers",
                empty_suffix,
                "name-starts-with-numbers",
                "name-starts-with-numbers",
                id="app_name_without_suffix_starts_with_numbers",
            ),
            pytest.param(
                "12345-name-starts-with-numbers",
                default_app_id_suffix,
                "name-starts-with-numbers",
                "name-starts-with-numbers-20240114121231",
                id="app_name_with_suffix_starts_with_numbers",
            ),
            pytest.param(
                None,
                empty_suffix,
                "spark-app",
                "spark-app",
                id="none_app_name_without_suffix",
            ),
            pytest.param(
                None,
                default_app_id_suffix,
                "spark-app-20240114121231",
                "spark-app-20240114121231",
                id="none_app_name_with_suffix",
            ),
            pytest.param(
                "custom-app-id-suffix",
                lambda: "-custom-app-id-suffix",
                "custom-app-id-suffix",
                "custom-app-id-suffix-custom-app-id-suffix",
                id="custom_app_id_suffix",
            ),
            pytest.param(
                "custom-dynamic-app-id-suffix",
                lambda: f"-{int(datetime.datetime.now().timestamp())}",
                "custom-dynamic-app-id-suffix",
                "custom-dynamic-app-id-suffix-1705234351",
                id="custom_dynamic_app_id_suffix",
            ),
        ],
    )
    @freeze_time(FAKE_TIME)
    def test_parse_app_name_and_id(
        self, app_name: str, app_id_suffix: Callable[[], str], expected_app_name: str, expected_app_id: str
    ):
        """
        Test the method _parse_app_name_and_id
        """
        spark_client = SparkOnK8S()
        actual_app_name, actual_app_id = spark_client._parse_app_name_and_id(
            app_name=app_name, app_id_suffix=app_id_suffix
        )
        assert actual_app_name == expected_app_name, "The app name is not as expected"
        assert actual_app_id == expected_app_id, "The app id is not as expected"
