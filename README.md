# Synapse Monitoring

Monitor Synapse Projects for new entities and notify specified users.  This package currently does the following:

1. Create a "Project Monitoring" Table in the specified project.  If the table already exists, it will simply get the existing table
2. Crawl through all synapse entities in the project to obtain each entity (except folders)
3. compared crawled list of files from step 2 with the table in step 1.
4. update table created in 1.
5. Send email to specified user if there are new entities


## Installation
```
git clone https://github.com/thomasyu888/synapse-monitor.git
cd synapse-monitor
pip install .
```

## Usage
```
synapsemonitor -h
synapsemonitor {projectid}
```