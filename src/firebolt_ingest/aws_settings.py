from typing import List, Optional, Tuple

from pydantic import BaseSettings, Field, SecretStr, root_validator


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
    s3_url: str = Field(
        min_length=1,
        max_length=255,
        regex=r"^s3:\/\/[a-z0-9-]{1,64}\/[a-zA-Z0-9-_.\/]*",
    )
    aws_credentials: Optional[AWSCredentials]


def generate_aws_credentials_string(creds: AWSCredentials) -> Tuple[str, List[str]]:
    """
        Prepares sql statement for passing the AWS credentials

    Args:
        creds: valid aws credentials

    Returns:
        a part of sql with the placeholders for the relevant credentials
        sequence of credentials
    """

    if creds.key_secret_creds is not None:
        ks_creds = creds.key_secret_creds
        return (
            "(AWS_KEY_ID = ? AWS_SECRET_KEY = ?)",
            [ks_creds.aws_key_id, ks_creds.aws_secret_key.get_secret_value()],
        )

    if creds.role_creds is not None:
        role_creds = creds.role_creds
        stmt = "(AWS_ROLE_ARN = ?{})".format(
            " AWS_ROLE_EXTERNAL_ID = ?" if role_creds.external_id else ""
        )
        params = [role_creds.role_arn.get_secret_value()] + (
            [role_creds.external_id] if role_creds.external_id else []
        )

        return stmt, params

    raise ValueError(
        "aws credentials should either have "
        "key_secret pair or role_creds; none is found"
    )
