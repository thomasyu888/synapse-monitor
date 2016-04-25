#!/usr/bin/env python

import synapseclient
import calendar
import time
import argparse
import multiprocessing.dummy as mp
import pandas as pd
import datetime
from synapseclient import Table

ONEDAY=86400000 #default delta t is 10 days prior

pd.options.mode.chained_assignment = None

def findNewFiles(args, id):
    """Performs query query to find changed entities in id. """

    QUERY = "select id, name, versionNumber, createdOn, createdByPrincipalId, nodeType, dataType from entity where projectId=='%s' and modifiedOn>%i" 
    t = calendar.timegm(time.gmtime())*1000
    project = syn.get(id)
    #Determine the last audit time or overide with lastTime
    if args.days is None:  #No time specified
        args.days = project.get('lastAuditTimeStamp', None)
        if args.days is None:  #No time specified and no lastAuditTimeStamp set
            args.days = t - ONEDAY*1.1
        else: #args.days came from annotation strip out from list
            args.days = args.days[0]  
    print t, args.days, id, (t-args.days)/float(ONEDAY), 'days'
    results = list(syn.chunkedQuery(QUERY % (id, args.days)))
    #Add the project and other metadata
    for r in results:
        r['projectId'] = id
        r['projectName'] = project.name
        r['date'] = synapseclient.utils.from_unix_epoch_time(r['entity.createdOn']).strftime("%b/%d/%Y %H:%M")
        r['user'] = syn.getUserProfile(r['entity.createdByPrincipalId'])['userName']
        r['type'] = r['entity.nodeType']

    #Set lastAuditTimeStamp
    if args.updateProject:
        project.lastAuditTimeStamp = t
        try:
            project = syn.store(project)
        except synapseclient.exceptions.SynapseHTTPError:
            pass
    return results

def composeMessage(entityList):
    """Composes a message with the contents of entityList """
    
    messageHead=('<h4>Time of Audit: %s </h4>'%time.ctime() +
                 '<table border=1><tr>'
                 '<th>Project</th>'
                 '<th>Entity</th>'
                 '<th>Ver.</th>'
                 '<th>Type</th>'
                 '<th>Change Time</th>'
                 '<th>Contributor</th></tr>'  )
    lines = [('<tr><td><a href="https://www.synapse.org/#!Synapse:%(projectId)s">%(projectName)s</a></td>'
              '<td><a href="https://www.synapse.org/#!Synapse:%(entity.id)s">(%(entity.id)s)</a> %(entity.name)s </td>'
              '<td>%(entity.versionNumber)s</td>'
              '<td>%(type)s</td>'
              '<td>%(date)s</td>'
              '<td><a href="https://www.synapse.org/#!Profile:%(entity.createdByPrincipalId)s">%(user)s</a></td></tr>')%item for 
             item in entityList]
    return messageHead + '\n'.join(lines)+'</table></body>'


def build_parser():
    """Set up argument parser and returns"""
    parser = argparse.ArgumentParser(
        description='Checks for new/modified entities in a project.')
    parser.add_argument('--userId', dest='userId',
                        help='User Id of individual to send report, defaults to current user.')
    parser.add_argument('--projects', '-p', metavar='projects', type=str, nargs='*',
            help='Synapse IDs of projects to be monitored.')
    parser.add_argument('--days', '-d', metavar='days', type=float, default=None,
            help='Find modifications in the last days')
    parser.add_argument('--updateProject', dest='updateProject',  action='store_true',
            help='If set will modify the annotations by setting lastAuditTimeStamp to the current time on each project.')
    parser.add_argument('--emailSubject', dest='emailSubject',  default = 'Updated Synapse Files',
            help='Sets the subject heading of the email sent out (defaults to Updated Synapse Files')
    parser.add_argument('--config', metavar='file', dest='configPath',  type=str,
            help='Synapse config file with user credentials (overides default ~/.synapseConfig)')
    return parser


p = mp.Pool(6)
args = build_parser().parse_args()
args.days = None if args.days is None else calendar.timegm(time.gmtime())*1000 - args.days*ONEDAY
if args.configPath is not None:
    syn=synapseclient.Synapse(skip_checks=True, configPath=args.configPath)
else:
    syn=synapseclient.Synapse(skip_checks=True)
syn.login(silent=True) 
args.userId = syn.getUserProfile()['ownerId'] if args.userId is None else args.userId


#query each project then combine into long list
entityList = p.map(lambda project: findNewFiles(args, project), args.projects)
entityList = [item for sublist in entityList for item in sublist]
#Filter out projects and folders
entityList = [e for e in entityList if e['entity.nodeType'] not in ['project', 'folder']]
print 'Total number of entities = ', len(entityList)

results = pd.DataFrame(entityList)

dataType = results['entity.dataType']
dataType[dataType.isnull()] = ''

for index,i in enumerate(dataType):
    if i!='':
        results['entity.dataType'][index] = i[0]

resultDf = pd.DataFrame(columns = ["project","entityName","changeTime","entityId","contributor","dataType"])
resultDf['project'] = results['projectName']
resultDf['entityName'] = results['entity.name']
resultDf['changeTime'] = results['entity.createdOn']
resultDf['entityId'] = results['entity.id']
resultDf['contributor'] = results['user']
resultDf['dataType'] = results['entity.dataType']


week_interval = ONEDAY * 6

third = args.days + week_interval*25
second = third + week_interval*4

schema = syn.get("syn5870873")
existingtable = syn.tableQuery("select * from syn5870873")
existingtable = existingtable.asDataFrame()
emptyDate = [str(i) for i in resultDf['changeTime']]
newTable = resultDf['project'] + resultDf['entityName'] + emptyDate
tableDate = [str(i) for i in existingtable['changeTime']]
oldTable = existingtable['project'] + existingtable['entityName'] + tableDate

upload = resultDf[~newTable.isin(oldTable)]
print("Updating Table")
syn.store(Table(schema, upload))

print("Updating wiki page")
wikipage = syn.getWiki("syn4939478",subpageId=396217)

#-d 180
markdown = ("#### _NTAP Project updates will be released here periodically_\n\n",
            "To see the most recent files added, please [see the Synapse Table](https://www.synapse.org/#!Synapse:syn5870873/tables/query/eyJsaW1pdCI6MjUsICJzcWwiOiJTRUxFQ1QgKiBGUk9NIHN5bjU4NzA4NzMgT1JERVIgQlkgXCJjaGFuZ2VUaW1lXCIgQVNDIiwgImlzQ29uc2lzdGVudCI6dHJ1ZSwgIm9mZnNldCI6MH0=)\n\n",
            "Summaries of the latest updates can be found below.\n\n",
            "###Last week\n",
            "**Projects**\n",
            "${synapsetable?query=SELECT project%2C COUNT%28%2A%29 FROM syn5870873 where ", 
            "changeTime > %d" % second,
            "GROUP BY project ORDER BY COUNT%28%2A%29 DESC&limit=5 }\n",
            "**Data Types**\n",
            "${synapsetable?query=SELECT dataType%2CCOUNT%28%2A%29 FROM syn5870873 where ",
            "changeTime > %d" %second,
            "GROUP BY dataType ORDER BY COUNT%28%2A%29 DESC&limit=5}\n",
            "###Last month\n",
            "**Projects**\n",
            "${synapsetable?query=SELECT project%2C COUNT%28%2A%29 FROM syn5870873 where ",
            "changeTime > %d" % third,
            "GROUP BY project ORDER BY COUNT%28%2A%29 DESC&limit=5}\n",
            "**Data Types**\n",
            "${synapsetable?query=SELECT dataType%2CCOUNT%28%2A%29 FROM syn5870873 where ",
            "changeTime > %d" %third,
            "GROUP BY dataType ORDER BY COUNT%28%2A%29 DESC&limit=5}\n",
            "###Last six months\n",
            "**Projects**\n",
            "${synapsetable?query=SELECT project%2C COUNT%28%2A%29 FROM syn5870873 where ",
            "changeTime > %d" % args.days,
            "GROUP BY project ORDER BY COUNT%28%2A%29 DESC&limit=5}\n",
            "**Data Types**\n",
            "${synapsetable?query=SELECT dataType%2CCOUNT%28%2A%29 FROM syn5870873 where ",
            "changeTime > %d" %args.days,
            "GROUP BY dataType ORDER BY COUNT%28%2A%29 DESC&limit=5}")
markdown = ''.join(markdown)
wikipage.markdown = markdown
syn.store(wikipage)

#Prepare and send Message
syn.sendMessage([args.userId], 
                args.emailSubject, 
                composeMessage(entityList),
                contentType = 'text/html')



