from typing import List, Iterator

from marshy.types import ExternalItemType
from ruamel.yaml import YAML

# noinspection PyProtectedMember
from servey.servey_aws.serverless.yml_config.yml_config_abc import (
    YmlConfigABC,
    ensure_ref_in_file,
    create_yml_file,
    _follow_path,
)

from persisty_data.finder.file_store_finder_abc import find_file_stores

_S3FileStore = "persisty_data.s3.s3_file_store.S3FileStore"


class S3YmlConfig(YmlConfigABC):
    """
    Set up some aspect of the serverless environment yml files. (For example, functions, resources, etc...)
    """

    s3_resource_yml_file: str = "serverless_servey/s3_resource.yml"
    s3_role_statement_yml_file: str = "serverless_servey/s3_role_statement.yml"

    def configure(self, main_serverless_yml_file: str):
        s3_stores = list(self.get_s3_stores())
        if not s3_stores:
            return
        ensure_ref_in_file(
            main_serverless_yml_file,
            ["resources"],
            self.s3_resource_yml_file,
        )
        ensure_ref_in_file(
            main_serverless_yml_file,
            ["provider", "iamRoleStatements"],
            self.s3_role_statement_yml_file,
            "iamRoleStatements.0",
        )
        self.add_s3_store_environment_variables(main_serverless_yml_file, s3_stores)
        s3_resource_yml = self.build_s3_resource_yml(s3_stores)
        create_yml_file(self.s3_resource_yml_file, s3_resource_yml)
        s3_role_statement_yml = self.build_s3_role_statement_yml(s3_stores)
        create_yml_file(self.s3_role_statement_yml_file, s3_role_statement_yml)

    @staticmethod
    def get_s3_stores() -> Iterator[_S3FileStore]:
        from persisty_data.s3.s3_file_store import S3FileStore

        file_stores = find_file_stores()
        for file_store in file_stores:
            if isinstance(file_store, S3FileStore):
                yield file_store

    @staticmethod
    def build_s3_resource_yml(s3_stores: List[_S3FileStore]) -> ExternalItemType:
        resources = {}
        for store in s3_stores:
            bucket_title = store.bucket_name.title().replace("-", "")
            resources[bucket_title] = {
                "Type": "AWS::S3::Bucket",
                "Properties": {"BucketName": store.bucket_name},
            }
        return {"Resources": resources}

    def build_s3_role_statement_yml(
        self, s3_stores: List[_S3FileStore]
    ) -> ExternalItemType:
        resources = []
        for store in s3_stores:
            resource_name = store.bucket_name.title().replace("-", "")
            resources.append({"Fn::GetAtt": [resource_name, "Arn"]})
        return {"iamRoleStatements": [self._iam_role_statement(resources)]}

    @staticmethod
    def _iam_role_statement(resource):
        result = {
            "Effect": "Allow",
            "Action": [
                "s3:AbortMultipartUpload",
                "s3:DeleteObject",
                "s3:DeleteObjectTagging",
                "s3:DeleteObjectVersion",
                "s3:DeleteObjectVersionTagging",
                "s3:GetObject",
                "s3:GetObjectAcl",
                "s3:GetObjectAttributes",
                "s3:GetObjectVersion",
                "s3:GetObjectVersionAcl",
                "s3:GetObjectVersionAttributes",
                "s3:ListBucket",
                "s3:ListBucketMultipartUploads",
                "s3:ListBucketVersions",
                "s3:ListMultipartUploadParts",
                "s3:PutBucketAcl",
                "s3:PutBucket",
            ],
            "Resource": resource,
        }
        return result

    @staticmethod
    def add_s3_store_environment_variables(
        main_serverless_yml_file: str, s3_stores: List[_S3FileStore]
    ):
        yaml = YAML()
        # pylint: disable=W1514
        with open(main_serverless_yml_file, "r") as reader:
            root = yaml.load(reader)
        parent = _follow_path(root, ["provider", "environment"])
        for store in s3_stores:
            name = f"PERSISTY_DATA_S3_BUCKET_{store.meta.name.upper()}"
            parent[name] = store.bucket_name
        # pylint: disable=W1514
        with open(main_serverless_yml_file, "w") as writer:
            yaml.dump(root, writer)
