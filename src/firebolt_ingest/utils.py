from typing import Optional

from pydantic import BaseSettings, SecretStr, root_validator


class AWSCredentialsKeySecret(BaseSettings):
    aws_key_id: str
    aws_secret_key: SecretStr


class AWSCredentialsRole(BaseSettings):
    role_arn: SecretStr
    external_id: Optional[str]


class AWSCredentials(BaseSettings):
    key_secret_creds: Optional[AWSCredentialsKeySecret]
    # or
    role_creds: Optional[AWSCredentialsRole]

    @root_validator
    def mutual_exclusive_with_creds(cls, values: dict) -> dict:
        if values["key_secret_creds"] and values["role_creds"]:
            raise ValueError(
                "Only one should be provided: key_secret_creds or role_arn"
            )

        if values["key_secret_creds"] is None and values["role_creds"] is None:
            raise ValueError(
                "At least one should be provided: key_secret_creds or role_arn"
            )

        return values


class AWSSettings(BaseSettings):
    # TODO: add s3 regex for the s3_url
    s3_url: str
    aws_credentials: Optional[AWSCredentials]


def generate_aws_credentials_string(creds: AWSCredentials) -> str:
    """

    :param creds:
    :return:
    """
    if creds.key_secret_creds is not None:
        ks_creds = creds.key_secret_creds
        return (
            f"(AWS_KEY_ID = '{ks_creds.aws_key_id}' "
            f"AWS_SECRET_KEY = '{ks_creds.aws_secret_key.get_secret_value()}')"
        )

    if creds.role_creds is not None:
        role_creds = creds.role_creds
        return f"(AWS_ROLE_ARN = '{role_creds.role_arn.get_secret_value()}'" + (
            f" AWS_ROLE_EXTERNAL_ID = '{role_creds.external_id}')"
            if role_creds.external_id
            else ")"
        )

    assert False
