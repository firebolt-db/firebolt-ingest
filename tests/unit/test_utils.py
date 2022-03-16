import pytest

from firebolt_ingest.utils import (
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

    try:
        AWSCredentials(key_secret_creds=key_secret_creds)
        AWSCredentials(role_creds=role_creds)
    except Exception:
        assert False


def test_generate_aws_credentials_string():
    """
    test all possible scenarios of generation of aws credentials string
    """

    key_secret_creds = AWSCredentialsKeySecret(
        aws_key_id="key_id", aws_secret_key="secret_key"
    )
    creds = generate_aws_credentials_string(
        AWSCredentials(key_secret_creds=key_secret_creds)
    )
    assert creds == "(AWS_KEY_ID = 'key_id' AWS_SECRET_KEY = 'secret_key')"

    role_creds = AWSCredentialsRole(role_arn="role_arn", external_id="external_id")
    creds = generate_aws_credentials_string(AWSCredentials(role_creds=role_creds))
    assert creds == "(AWS_ROLE_ARN = 'role_arn' AWS_ROLE_EXTERNAL_ID = 'external_id')"

    role_creds = AWSCredentialsRole(role_arn="role_arn")
    creds = generate_aws_credentials_string(AWSCredentials(role_creds=role_creds))
    assert creds == "(AWS_ROLE_ARN = 'role_arn')"
