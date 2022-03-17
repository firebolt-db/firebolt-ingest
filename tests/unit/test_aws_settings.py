import pytest

from firebolt_ingest.aws_settings import (
    AWSCredentials,
    AWSCredentialsKeySecret,
    AWSCredentialsRole,
    generate_aws_credentials_string,
)


def test_aws_credentials():
    """
    test creating different AWSCredentials,
    also test mutual exclusiveness of role_arn and key_secret
    """
    role_creds = AWSCredentialsRole(role_arn="role_arn", external_id="external_id")
    key_secret_creds = AWSCredentialsKeySecret(
        aws_key_id="key_id", aws_secret_key="secret_key"
    )

    with pytest.raises(ValueError):
        AWSCredentials()

    with pytest.raises(ValueError):
        AWSCredentials(role_creds=role_creds, key_secret_creds=key_secret_creds)

    assert (
        AWSCredentials(key_secret_creds=key_secret_creds).key_secret_creds
        == key_secret_creds
    )
    assert AWSCredentials(role_creds=role_creds).role_creds == role_creds


def test_generate_aws_credentials_string():
    """
    test all possible scenarios of generation of aws credentials string
    """

    key_secret_creds = AWSCredentialsKeySecret(
        aws_key_id="key_id", aws_secret_key="secret_key"
    )
    stmt, params = generate_aws_credentials_string(
        AWSCredentials(key_secret_creds=key_secret_creds)
    )
    assert stmt == "(AWS_KEY_ID = ? AWS_SECRET_KEY = ?)"
    assert params == ["key_id", "secret_key"]

    role_creds = AWSCredentialsRole(role_arn="role_arn", external_id="external_id")
    stmt, params = generate_aws_credentials_string(
        AWSCredentials(role_creds=role_creds)
    )
    assert stmt == "(AWS_ROLE_ARN = ? AWS_ROLE_EXTERNAL_ID = ?)"
    assert params == ["role_arn", "external_id"]

    role_creds = AWSCredentialsRole(role_arn="role_arn")
    stmt, params = generate_aws_credentials_string(
        AWSCredentials(role_creds=role_creds)
    )
    assert stmt == "(AWS_ROLE_ARN = ?)"
    assert params == ["role_arn"]
