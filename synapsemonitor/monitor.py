"""Monitor module"""
import pandas as pd
import synapseclient
from synapsegenie import bootstrap, input_to_database, process_functions


def create_tracking_table(syn, parent):
    """Set up the table that will track project entities"""
    status_table_col_defs = [
        {'name': 'id',
         'columnType': 'ENTITYID'},
        {'name': 'md5',
         'columnType': 'STRING',
         'maximumSize': 1000},
        {'name': 'name',
         'columnType': 'STRING',
         'maximumSize': 1000},
        {'name': 'modifiedon',
         'columnType': 'DATE'}
    ]
    return bootstrap._create_table(syn, name="Project Monitoring",
                                   col_config=status_table_col_defs,
                                   parent=parent)


def check_entity(entity, tracking_table):
    """Checks entity with tracking table

    Args:
        tracking_table: Tracking Synapse Table query result
        entity: Synapse Entity

    Returns:
        list - [entity Id, entity md5, entity name, entity modified on,
                entity status]
    """
    tracking_tabledf = tracking_table.asDataFrame()

    # Get the current status and errors from the tables.
    entity_exists = tracking_tabledf[tracking_tabledf['id'] == entity.id]
    status = "Existing"

    if entity_exists.empty:
        status = "New"
    else:
        # Check if md5 is different if md5 exists
        diff_md5 = entity_exists['md5'].values[0] != entity.get("md5", 'NA')
        # Check if entity name is different
        diff_name = entity_exists['name'].values[0] != entity.name
        if diff_md5 or diff_name:
            status = "Updated"

    return [entity.id, entity.get("md5", 'NA'), entity.name,
            input_to_database.entity_date_to_timestamp(
                entity.properties.modifiedOn
            ),
            status]


def monitoring(projectid, synapseconfig=None, userid=None,
               email_subject="New Synapse Files"):
    """Invoke monitoring"""
    # Log into synapse
    if synapseconfig is not None:
        syn = synapseclient.Synapse(skip_checks=True, configPath=synapseconfig)
    else:
        syn = synapseclient.Synapse(skip_checks=True)
    syn.login(silent=True)

    # Obtain user id
    userid = syn.getUserProfile()['ownerId'] if userid is None else userid
    username = syn.getUserProfile()['userName']
    # get files of synapse project
    project_ent = syn.get(projectid)

    # Get list of entities
    entity_list = input_to_database.get_center_input_files(
        syn=syn, synid=projectid, center=project_ent.name,
        downloadFile=False
    )

    print(f'Total number of entities = {len(entity_list) - 1}')

    # Create tracking table, gets table if already exists
    tracking_table = create_tracking_table(syn, projectid)
    tracking_table = syn.tableQuery(f"select * from {tracking_table.id}")
    # Create entity tracking list
    current_tracking_list = []
    for entity_lst in entity_list:
        # Artifact of project GENIE, where 'get_center_input_files' returns
        # list of lists
        entity = entity_lst[0]
        # No need to add in 'Project Monitoring' table
        if entity.name == "Project Monitoring":
            continue
        current_tracking_list.append(check_entity(entity, tracking_table))

    columns = ['id', 'md5', 'name', 'modifiedon', 'status']
    current_trackingdf = pd.DataFrame(current_tracking_list, columns=columns)

    # Update tracking table
    process_functions.updateDatabase(
        syn=syn, database=tracking_table.asDataFrame(),
        new_dataset=current_trackingdf, database_synid=tracking_table.tableId,
        primary_key_cols=['id'], to_delete=True
    )

    # Send email only if there are new files
    new_files_idx = current_trackingdf['status'] == "New"
    new_filesdf = current_trackingdf[new_files_idx][['id', 'name']]

    if not new_filesdf.empty:
        message = (
            f"Hello {username},<br/><br/>"
            "These are the new synapse entities for "
            f"synapse project: <a href='https://www.synapse.org/#!Synapse:{project_ent.id}'>{project_ent.name}</a>"
            "<br/><br/>"
        )
        syn.sendMessage([userid], email_subject,
                        message+new_filesdf.to_html(index=False),
                        contentType = 'text/html')
