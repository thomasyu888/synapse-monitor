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


def findNewFiles(args, id):
    """Performs query query to find changed entities in id. """

    QUERY = "select id, name, versionNumber, modifiedOn, modifiedByPrincipalId, nodeType from entity where projectId=='%s' and modifiedOn>%i" 
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
        r['date'] = synapseclient.utils.from_unix_epoch_time(r['entity.modifiedOn']).strftime("%b/%d/%Y %H:%M")
        r['user'] = syn.getUserProfile(r['entity.modifiedByPrincipalId'])['userName']
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
              '<td><a href="https://www.synapse.org/#!Profile:%(entity.modifiedByPrincipalId)s">%(user)s</a></td></tr>')%item for 
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
resultDf = pd.DataFrame(columns = ["project","entityName","changeTime","entityId","contributor"])
resultDf['project'] = results['projectName']
resultDf['entityName'] = results['entity.name']
resultDf['changeTime'] = results['entity.modifiedOn']
resultDf['entityId'] = results['entity.id']
resultDf['contributor'] = results['user']

week_interval = ONEDAY * 7

third = args.days + week_interval + 1
second = third + week_interval

#3rd week
firstdate = synapseclient.utils.from_unix_epoch_time(args.days).strftime("%b/%d/%Y")
#2nd week
secondstartdate = synapseclient.utils.from_unix_epoch_time(third).strftime("%b/%d/%Y")
secondenddate = synapseclient.utils.from_unix_epoch_time(third - ONEDAY).strftime("%b/%d/%Y")
#1st week
thirdstartdate = synapseclient.utils.from_unix_epoch_time(second).strftime("%b/%d/%Y")
thirdenddate = synapseclient.utils.from_unix_epoch_time(second - ONEDAY).strftime("%b/%d/%Y")
present = synapseclient.utils.from_unix_epoch_time(calendar.timegm(time.gmtime())*1000).strftime("%b/%d/%Y")

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
wikipage = syn.getWiki("syn4990358",subpageId=396210)

markdown = ("#### _NTAP Project updates will be released here periodically_\n\n",
            "###%s to %s\n" % (thirdstartdate, present),
            "**Contributors**\n",
            "${synapsetable?query=SELECT contributor%2C COUNT%28%2A%29 FROM syn5870873 where ", 
            "changeTime > %d" % second,
            "&limit=5 GROUP BY contributor ORDER BY COUNT%28%2A%29 DES}\n",
            "**Projects updated**",
            "${synapsetable?query=SELECT project%2C COUNT%28%2A%29 FROM syn5870873 where ",
            "changeTime > %d" % second,
            "&limit=5 GROUP BY contributor ORDER BY COUNT%28%2A%29 DES}\n",
            "**Activity**\n",
            "${synapsetable?query=SELECT project%2CentityName%2CentityId%2Ccontributor FROM syn5870873 where ",
            "changeTime > %d&limit=5}\n\n" % second,
            "###%s to %s\n" % (secondstartdate,thirdenddate),
            "**Contributors**\n",
            "${synapsetable?query=SELECT contributor%2C COUNT%28%2A%29 FROM syn5870873 where ",
            "changeTime > %d and changeTime < %d" % (third,second),
            "&limit=5 GROUP BY contributor ORDER BY COUNT%28%2A%29 DES}\n",
            "**Projects updated**",
            "${synapsetable?query=SELECT project%2C COUNT%28%2A%29 FROM syn5870873 where ",
            "changeTime > %d and changeTime < %d" % (third,second),
            "&limit=5 GROUP BY contributor ORDER BY COUNT%28%2A%29 DES}\n",
            "**Activity**\n",
            "${synapsetable?query=SELECT project%2CentityName%2CentityId%2Ccontributor FROM syn5870873 where ",
            "changeTime > %d and changeTime < %d&limit=5}\n\n" %(third, second),
            "###%s to %s\n" % (firstdate, secondenddate),
            "**Contributors**\n",
            "${synapsetable?query=SELECT contributor%2C COUNT%28%2A%29 FROM syn5870873 where ",
            "changeTime < %d" % third,
            "&limit=5 GROUP BY contributor ORDER BY COUNT%28%2A%29 DES}\n",
            "**Projects updated**",
            "${synapsetable?query=SELECT project%2C COUNT%28%2A%29 FROM syn5870873 where ",
            "changeTime < %d" % third,
            "&limit=5 GROUP BY contributor ORDER BY COUNT%28%2A%29 DES}\n",
            "**Activity**\n",
            "${synapsetable?query=SELECT project%2CentityName%2CentityId%2Ccontributor FROM syn5870873 where ",
            "changeTime < %d&limit=5}" % third)

markdown = ''.join(markdown)
wikipage.markdown = markdown
syn.store(wikipage)


#Prepare and send Message
syn.sendMessage([args.userId], 
                args.emailSubject, 
                composeMessage(entityList),
                contentType = 'text/html')



