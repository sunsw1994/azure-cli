# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

from knack.util import CLIError
from ._client_factory import cf_synapse_client_workspace_factory
from azure.cli.core.util import sdk_no_wait, read_file_content
from azure.mgmt.synapse.models import Workspace, WorkspacePatchInfo, ManagedIdentity, \
    DataLakeStorageAccountDetails, \
    BigDataPoolResourceInfo, AutoScaleProperties, AutoPauseProperties, LibraryRequirements, NodeSizeFamily, \
    SqlPool, SqlPoolPatchInfo, Sku
from .constant import SynapseSqlCreateMode

# Synapse workspace
def list_workspaces(cmd, client, resource_group_name=None):  # pylint: disable=unused-argument
    return client.list_by_resource_group(
        resource_group_name=resource_group_name) if resource_group_name else client.list()


def create_workspace(cmd, client, resource_group_name, workspace_name, storage_account, file_system,
                     sql_admin_login_user, sql_admin_login_password, location, tags=None, no_wait=False):
    identity_type = "SystemAssigned"
    identity = ManagedIdentity(type=identity_type)
    account_url = "https://{}.dfs.{}".format(storage_account, cmd.cli_ctx.cloud.suffixes.storage_endpoint)
    default_data_lake_storage = DataLakeStorageAccountDetails(account_url=account_url, filesystem=file_system)
    workspace_info = Workspace(
        identity=identity,
        default_data_lake_storage=default_data_lake_storage,
        sql_administrator_login=sql_admin_login_user,
        sql_administrator_login_password=sql_admin_login_password,
        location=location
    )
    return sdk_no_wait(no_wait, client.create_or_update, resource_group_name, workspace_name, workspace_info)


def update_workspace(cmd, client, resource_group_name, workspace_name, sql_admin_login_password=None,
                     tags=None, no_wait=False):
    workspace_patch_info = WorkspacePatchInfo(tags=tags, sql_admin_login_password=sql_admin_login_password)
    return sdk_no_wait(no_wait, client.update, resource_group_name, workspace_name, workspace_patch_info)

def custom_check_name_availability(cmd, client, name):
    return client.check_name_availability(name, "Microsoft.Synapse/workspaces")

# Synapse sparkpool
def get_spark_pool(cmd, client, resource_group_name, workspace_name, spark_pool_name):
    return client.get(resource_group_name, workspace_name, spark_pool_name)

def create_spark_pool(cmd, client, resource_group_name, workspace_name, spark_pool_name,
                      spark_version, node_size, node_count,
                      node_size_family=NodeSizeFamily.memory_optimized.value, enable_auto_scale=None,
                      min_node_count=None, max_node_count=None,
                      enable_auto_pause=None, delay=None, spark_events_folder="/events",
                      library_requirements_file=None,
                      default_spark_log_folder="/logs", tags=None, no_wait=False):

    workspace_client = cf_synapse_client_workspace_factory(cmd.cli_ctx)
    workspace_object = workspace_client.get(resource_group_name, workspace_name)
    location = workspace_object.location

    big_data_pool_info = BigDataPoolResourceInfo(location=location, spark_version=spark_version, node_size=node_size,
                                                 node_count=node_count, node_size_family=node_size_family,
                                                 spark_events_folder=spark_events_folder,
                                                 default_spark_log_folder=default_spark_log_folder, tags=tags)

    big_data_pool_info.auto_scale = AutoScaleProperties(enabled=enable_auto_scale, min_node_count=min_node_count,
                                                        max_node_count=max_node_count)

    big_data_pool_info.auto_pause = AutoPauseProperties(enabled=enable_auto_pause,
                                                        delay_in_minutes=delay)

    if library_requirements_file:
        library_requirements_content = read_file_content(library_requirements_file)
        big_data_pool_info.library_requirements = LibraryRequirements(filename=library_requirements_file,
                                                                      content=library_requirements_content)
    return sdk_no_wait(no_wait, client.create_or_update, resource_group_name, workspace_name, spark_pool_name,
                       big_data_pool_info)


def update_spark_pool(cmd, client, resource_group_name, workspace_name, spark_pool_name,
                      node_size=None, node_count=None, enable_auto_scale=None,
                      min_node_count=None, max_node_count=None,
                      enable_auto_pause=None, delay=None,
                      library_requirements_file=None, tags=None, force=False, no_wait=False):
    existing_spark_pool = client.get(resource_group_name, workspace_name, spark_pool_name)

    if node_size:
        existing_spark_pool.node_size = node_size
    if node_count:
        existing_spark_pool.node_count = node_count

    if library_requirements_file:
        library_requirements_content = read_file_content(library_requirements_file)
        existing_spark_pool.library_requirements = LibraryRequirements(filename=library_requirements_file,
                                                                       content=library_requirements_content)
    if tags:
        existing_spark_pool.tags = tags

    if existing_spark_pool.auto_scale is not None:
        if enable_auto_scale is not None:
            existing_spark_pool.auto_scale.enabled = enable_auto_scale
        if min_node_count:
            existing_spark_pool.auto_scale.min_node_count = min_node_count
        if max_node_count:
            existing_spark_pool.auto_scale.max_node_count = max_node_count
    else:
        existing_spark_pool.auto_scale = AutoScaleProperties(enabled=enable_auto_scale, min_node_count=min_node_count,
                                                             max_node_count=max_node_count)

    if existing_spark_pool.auto_pause is not None:
        if enable_auto_pause is not None:
            existing_spark_pool.auto_pause.enabled = enable_auto_pause
        if delay:
            existing_spark_pool.auto_pause.delay_in_minutes = delay
    else:
        existing_spark_pool.auto_pause = AutoPauseProperties(enabled=enable_auto_pause,
                                                             delay_in_minutes=delay)

    return sdk_no_wait(no_wait, client.create_or_update, resource_group_name, workspace_name, spark_pool_name,
                       existing_spark_pool, force=force)


def delete_spark_pool(cmd, client, resource_group_name, workspace_name, spark_pool_name, no_wait=False):
    return sdk_no_wait(no_wait, client.delete, resource_group_name, workspace_name, spark_pool_name)


# Synapse sqlpool
def create_sql_pool(cmd, client, resource_group_name, workspace_name, sql_pool_name, performance_level, tags=None,
                    no_wait=False):
    workspace_client = cf_synapse_client_workspace_factory(cmd.cli_ctx)
    workspace_object = workspace_client.get(resource_group_name, workspace_name)
    location = workspace_object.location

    sku = Sku(name=performance_level)

    sql_pool_info = SqlPool(sku=sku, location=location, create_mode=SynapseSqlCreateMode.Default, tags=tags)

    return sdk_no_wait(no_wait, client.create, resource_group_name, workspace_name, sql_pool_name, sql_pool_info)


def update_sql_pool(cmd, client, resource_group_name, workspace_name, sql_pool_name, sku_name=None, tags=None):
    sku = Sku(name=sku_name)
    sql_pool_patch_info = SqlPoolPatchInfo(sku=sku, tags=tags)
    return client.update(resource_group_name, workspace_name, sql_pool_name, sql_pool_patch_info)


def create_firewall_rule(cmd, client, resource_group_name, workspace_name, rule_name, start_ip_address, end_ip_address,
                         no_wait=False):
    return sdk_no_wait(no_wait, client.create_or_update, resource_group_name, workspace_name, rule_name,
                       start_ip_address=start_ip_address, end_ip_address=end_ip_address)

