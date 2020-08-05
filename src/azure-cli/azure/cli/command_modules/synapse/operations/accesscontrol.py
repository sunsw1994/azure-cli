# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

from knack.util import CLIError
from .._client_factory import cf_synapse_client_accesscontrol_factory
from .._client_factory import cf_graph_client_factory
from azure.synapse.accesscontrol.models import RoleAssignmentOptions
from azure.cli.core.util import is_guid


# List Synapse Role Assignment
def list_role_assignments(cmd, workspace_name, role=None, object_id=None,
                          service_principal_name=None, user_principal_name=None):
    # get role id
    role_id = _resolve_role_id(cmd, role, workspace_name)

    # get object_id
    if object_id:
        if bool(service_principal_name) or bool(user_principal_name):
            raise CLIError("--service_principal_name or --user_principal_name are not required when --object_id is used.")
    else:
        if bool(service_principal_name) and bool(user_principal_name):
            raise CLIError("--service_principal_name and --user_principal_name arguments should not be provided at the same time.")
        object_id = _resolve_object_id(cmd, user_principal_name, service_principal_name)

    client = cf_synapse_client_accesscontrol_factory(cmd.cli_ctx, workspace_name)
    role_assignments = client.get_role_assignments(role_id, object_id)

    # TODO:
    # Currently, when only `ObjectId` is specified, the cmdlet returns incorrect result.
    # Filter from client side as a workaround
    if object_id:
        role_assignments = [x for x in role_assignments if x.principal_id == object_id]
    return role_assignments


# Show Synapse Role Assignment By Id
def get_role_assignment_by_id(cmd, workspace_name, role_assignment_id):
    client = cf_synapse_client_accesscontrol_factory(cmd.cli_ctx, workspace_name)
    return client.get_role_assignment_by_id(role_assignment_id)


# Delete Synapse Role Assignment By Id
def delete_role_assignment_by_id(cmd, workspace_name, role_assignment_id):
    client = cf_synapse_client_accesscontrol_factory(cmd.cli_ctx, workspace_name)
    return client.delete_role_assignment_by_id(role_assignment_id)


# Create Synapse Role Assignment
def create_role_assignment(cmd, workspace_name, role,
                           object_id=None, service_principal_name=None, user_principal_name=None):
    # get role id
    role_id = _resolve_role_id(cmd, role, workspace_name)

    # get object_id
    if object_id:
        if bool(service_principal_name) or bool(user_principal_name):
            raise CLIError("You should provide either --object_id or --service_principal_name/--user_principal_name arguments.")
    else:
        if not (bool(service_principal_name) ^ bool(user_principal_name)):
            raise CLIError("You should provide either --object_id or --service_principal_name/--user_principal_name arguments.")
        object_id = _resolve_object_id(cmd, user_principal_name, service_principal_name)

    create_role_assignment_options = RoleAssignmentOptions(
        role_id=role_id,
        principal_id=object_id
    )
    assignment_client = cf_synapse_client_accesscontrol_factory(cmd.cli_ctx, workspace_name)
    return assignment_client.create_role_assignment(create_role_assignment_options)


# List Synapse Role Definitions
def list_role_definitions(cmd, workspace_name):
    client = cf_synapse_client_accesscontrol_factory(cmd.cli_ctx, workspace_name)
    return client.get_role_definitions()


# Get Synapse Role Definition
def get_role_definition(cmd, workspace_name, role):
    role_id = _resolve_role_id(cmd, role, workspace_name)
    client = cf_synapse_client_accesscontrol_factory(cmd.cli_ctx, workspace_name)
    return client.get_role_definition_by_id(role_id)


def _get_service_principal(graph_client, service_principal_name):
    query_exp = 'servicePrincipalNames/any(x:x eq \'{}\')'.format(service_principal_name)
    aad_sps = list(graph_client.service_principals.list(filter=query_exp))
    if not aad_sps:
        raise CLIError("Service Principal Name '{}' doesn't exist.".format(service_principal_name))
    return aad_sps[0].object_id if aad_sps else None


def _resolve_role_id(cmd, role, workspace_name):
    role_id = None
    if not role:
        return role_id
    if is_guid(role):
        role_id = role
    else:
        role_definition_client = cf_synapse_client_accesscontrol_factory(cmd.cli_ctx, workspace_name)
        role_definition = role_definition_client.get_role_definitions()
        role_dict = {x.name.lower(): x.id for x in role_definition if x.name}
        if role.lower() not in role_dict:
            raise CLIError("Role '{}' doesn't exist.".format(role))
        role_id = role_dict[role.lower()]
    return role_id


def _resolve_object_id(cmd, user_principal_name, service_principal_name):
    object_id = None
    if user_principal_name:
        graph_client = cf_graph_client_factory(cmd.cli_ctx)
        object_id = graph_client.users.get(user_principal_name).object_id
    if service_principal_name:
        if 'http://' not in service_principal_name and not is_guid(service_principal_name):
            service_principal_name = "http://" + service_principal_name
        graph_client = cf_graph_client_factory(cmd.cli_ctx)
        object_id = _get_service_principal(graph_client, service_principal_name)
    return object_id
